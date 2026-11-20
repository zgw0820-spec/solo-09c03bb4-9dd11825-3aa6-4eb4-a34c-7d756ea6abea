"""磁盘存储原语：内容寻址块、原子写、目录 fsync、启动恢复。

布局（DATA_DIR 下）::

    manifest.json          链清单（权威的顺序/父子关系），tmp 暂存 + rename 原子替换
    snapshots/<id>.json    快照元数据（disk_size + 块下标 -> 块哈希）
    blocks/ab/<64hex>      内容寻址块（SHA-256 即文件名），永不原地修改
    pins/<pin_id>          在途恢复钉住的快照 id，一个 pin 一个文件
    tmp/                   临时文件目录，启动时整体清空

崩溃安全原则：
* 块只写新文件（同哈希即同内容），先写 tmp 再 fsync、rename、fsync 目录；
* 快照 JSON / 清单都是「写临时文件 -> fsync -> rename -> fsync 目录」，
  rename 在同一文件系统上是原子的，进程中断后只会看到完整的旧文件或新文件；
* 切换永远先写新快照 JSON，后写新清单，因此清单引用的快照必然完整；
* 启动时清理 tmp，并以清单为唯一权威做对账（多余的快照/块文件留给 GC 回收）。
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

BLOCK_SUFFIX = ".bin"


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """字节原子写：同目录 tmp 文件 + fsync + rename + 目录 fsync。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def atomic_write_json(path: Path, obj: Any) -> None:
    atomic_write_bytes(path, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"))


def read_json(path: Path) -> Any:
    with open(path, "rb") as fh:
        return json.loads(fh.read().decode("utf-8"))


def block_relpath(digest: str) -> Path:
    return Path(digest[:2]) / f"{digest}{BLOCK_SUFFIX}"


def block_path(root: Path, digest: str) -> Path:
    return root / block_relpath(digest)


class BlockStore:
    """内容寻址块仓库。块不可变，重复写入同哈希块是空操作。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.tmp_root = root.parent / "tmp"
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp_root.mkdir(parents=True, exist_ok=True)

    def put(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        final = block_path(self.root, digest)
        if final.exists():
            return digest
        final.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.tmp_root / f"block-{uuid.uuid4().hex}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            os.write(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, final)
        _fsync_dir(final.parent)
        return digest

    def get(self, digest: str) -> bytes:
        with open(block_path(self.root, digest), "rb") as fh:
            return fh.read()

    def open_read(self, digest: str):
        return open(block_path(self.root, digest), "rb")

    def exists(self, digest: str) -> bool:
        return block_path(self.root, digest).exists()

    def all_hashes(self) -> set[str]:
        hashes: set[str] = set()
        for shard in sorted(self.root.iterdir()):
            if not shard.is_dir():
                continue
            for entry in shard.iterdir():
                if entry.is_file() and entry.name.endswith(BLOCK_SUFFIX):
                    hashes.add(entry.name[: -len(BLOCK_SUFFIX)])
        return hashes

    def delete(self, digest: str) -> None:
        target = block_path(self.root, digest)
        try:
            target.unlink()
        except FileNotFoundError:
            return
        _fsync_dir(target.parent)


def prepare_dirs(settings_dirs: list[Path]) -> None:
    for directory in settings_dirs:
        directory.mkdir(parents=True, exist_ok=True)


def clean_tmp(tmp_dir: Path) -> None:
    """启动时清空 tmp：残留只可能来自中断的写入，没有任何提交物引用它们。"""
    if not tmp_dir.exists():
        return
    for entry in tmp_dir.iterdir():
        if entry.is_file() or entry.is_symlink():
            entry.unlink()
        else:
            _rmtree(entry)
    _fsync_dir(tmp_dir)


def _rmtree(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            _rmtree(child)
        else:
            child.unlink()
    path.rmdir()
