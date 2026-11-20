"""备份服务核心：快照链、分块上传、增量合并、流式恢复、块回收。

并发模型（单进程多线程，FastAPI 同步路由走线程池）：
* ``_chain_lock`` 保护清单与快照元数据的读改写；
* ``_upload_lock`` 串行化整次上传（块去重本身幂等，串行化让链尾线性追加最简单可靠）；
* ``_merge_lock`` 串行化合并；合并的准备工作（读链、生成新元数据、GC 标记）
  不持有链锁，只在最后原子切换清单的瞬间持有，因此合并期间链尾可以追加快照；
* 在途恢复通过 pins 目录里的 pin 文件钉住快照，GC 只回收「无快照引用且无 pin」的块。

块下标语义：
* 上传里出现的下标 = 本快照显式覆盖（显式全零块也是普通块，照常覆盖）；
* 未出现的下标 = 沿用父快照；全量备份要求覆盖全部下标。
"""
from __future__ import annotations

import io
import json
import os
import struct
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator, Optional

from .config import Settings
from .models import ChainEntry, Manifest, SnapshotRecord
from .storage import (
    BlockStore,
    atomic_write_json,
    clean_tmp,
    prepare_dirs,
    read_json,
)

CHUNK_HEADER = struct.Struct(">II")  # index(uint32 BE), length(uint32 BE)
MAX_INDEX = 0xFFFFFFFF


class BackupError(Exception):
    """带 HTTP 状态码的业务异常。"""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class RestorePin:
    """在途恢复句柄：存在期间对应快照引用的块不会被 GC。"""

    def __init__(self, pins_dir, snapshot_id: str, pin_id: str) -> None:
        self._pins_dir = pins_dir
        self.snapshot_id = snapshot_id
        self.pin_id = pin_id
        self._path = pins_dir / pin_id
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._path.unlink()
        except FileNotFoundError:
            pass

    def __enter__(self) -> "RestorePin":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class BackupService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        prepare_dirs(
            [
                settings.data_dir,
                settings.blocks_dir,
                settings.snapshots_dir,
                settings.pins_dir,
                settings.tmp_dir,
            ]
        )
        # 启动恢复第一步：清掉所有中断写入留下的 tmp 文件。
        clean_tmp(settings.tmp_dir)
        self.blocks = BlockStore(settings.blocks_dir)
        self._chain_lock = threading.RLock()
        self._upload_lock = threading.Lock()
        self._merge_lock = threading.Lock()
        self.manifest: Manifest = self._load_manifest()
        self._reconcile_on_startup()

    # ------------------------------------------------------------------ 清单

    def _load_manifest(self) -> Manifest:
        path = self.settings.manifest_path
        if not path.exists():
            return Manifest(chain=[])
        raw = read_json(path)
        manifest = Manifest.model_validate(raw)
        return manifest

    def _save_manifest_locked(self) -> None:
        atomic_write_json(self.settings.manifest_path, self.manifest.model_dump())

    def _snapshot_path(self, snapshot_id: str):
        return self.settings.snapshots_dir / f"{snapshot_id}.json"

    def _load_snapshot(self, snapshot_id: str) -> SnapshotRecord:
        path = self._snapshot_path(snapshot_id)
        if not path.exists():
            raise BackupError(404, f"快照 {snapshot_id} 的元数据文件缺失")
        try:
            return SnapshotRecord.model_validate(read_json(path))
        except (json.JSONDecodeError, OSError) as exc:
            raise BackupError(500, f"快照 {snapshot_id} 元数据损坏: {exc}") from exc

    def _write_snapshot(self, record: SnapshotRecord) -> None:
        # 先落快照 JSON（原子），后改清单，保证清单永远不会指向残缺快照。
        atomic_write_json(self._snapshot_path(record.id), record.model_dump())

    def _chain_ids_locked(self) -> list[str]:
        return [entry.id for entry in self.manifest.chain]

    def _index_locked(self, snapshot_id: str) -> int:
        for i, entry in enumerate(self.manifest.chain):
            if entry.id == snapshot_id:
                return i
        raise BackupError(404, f"快照 {snapshot_id} 不在链上")

    def _get_entry_locked(self, snapshot_id: str) -> ChainEntry:
        return self.manifest.chain[self._index_locked(snapshot_id)]

    def _tail_id_locked(self) -> Optional[str]:
        if not self.manifest.chain:
            return None
        return self.manifest.chain[-1].id

    # ---------------------------------------------------------- 启动崩溃恢复

    def _reconcile_on_startup(self) -> None:
        """以清单为唯一权威对账，保证重启后只看得到完整的旧链或新链。"""
        with self._chain_lock:
            chain_ids = set(self._chain_ids_locked())
            removed_orphans = 0
            for path in self.settings.snapshots_dir.glob("*.json"):
                if path.stem not in chain_ids:
                    # 中断在「快照已写、清单未切换」的窗口：它不属于任何链。
                    path.unlink()
                    removed_orphans += 1
            # 校正快照文件里的 parent_id，使其与清单一致（合并切换可能停在两步之间）。
            for entry in self.manifest.chain:
                snap = self._load_snapshot(entry.id)
                if snap.parent_id != entry.parent_id:
                    snap.parent_id = entry.parent_id
                    self._write_snapshot(snap)
            # 上次进程的在途恢复全部失效，pin 清空（块仍由快照引用，不会误删）。
            for pin in self.settings.pins_dir.glob("*.pin"):
                pin.unlink()
            self.gc_locked()
            if removed_orphans:
                # 仅用于启动日志可见性。
                print(
                    f"[startup] 清理 {removed_orphans} 个未提交的快照元数据文件",
                    flush=True,
                )

    # -------------------------------------------------------------- 分块上传

    @staticmethod
    def _iter_chunks(stream: io.BytesIO) -> Iterator[tuple[int, bytes]]:
        """解析 ``index(u32BE) + length(u32BE) + data`` 帧序列。"""
        header_size = CHUNK_HEADER.size
        while True:
            header = stream.read(header_size)
            if not header:
                return
            if len(header) != header_size:
                raise BackupError(400, "分块流被截断：帧头不完整")
            index, length = CHUNK_HEADER.unpack(header)
            data = stream.read(length)
            if len(data) != length:
                raise BackupError(400, f"分块流被截断：块 {index} 数据不完整")
            yield index, data

    def create_snapshot(
        self,
        parent_id: Optional[str],
        disk_size: int,
        block_size: int,
        chunk_stream: io.BytesIO,
    ) -> SnapshotRecord:
        if block_size != self.settings.block_size:
            raise BackupError(
                400,
                f"块大小必须为 {self.settings.block_size} 字节，收到 {block_size}",
            )
        if disk_size < 0:
            raise BackupError(400, "disk_size 不能为负数")

        with self._upload_lock:
            # 第一步：纯请求校验与分块解析（不依赖链状态），
            # 请求自身不合法时一律 400，优先于链冲突的 409。
            expected_blocks = (
                (disk_size + block_size - 1) // block_size if disk_size else 0
            )
            blocks: dict[str, str] = {}
            seen: set[int] = set()
            for index, data in self._iter_chunks(chunk_stream):
                if index >= expected_blocks:
                    raise BackupError(
                        400,
                        f"块下标 {index} 越界（盘大小 {disk_size} 共 {expected_blocks} 块）",
                    )
                if index in seen:
                    raise BackupError(400, f"块下标 {index} 在同一备份中重复出现")
                if index > MAX_INDEX:  # 防御性，struct 已限 u32
                    raise BackupError(400, f"块下标 {index} 超出 uint32 范围")
                seen.add(index)
                # 块长度校验：除最后一块按 disk_size 截断外，其余必须恰好 block_size。
                expected_len = (
                    disk_size - index * block_size
                    if index == expected_blocks - 1
                    else block_size
                )
                if len(data) != expected_len:
                    raise BackupError(
                        400,
                        f"块 {index} 长度应为 {expected_len} 字节，实际 {len(data)} 字节",
                    )
                # 显式全零块就是普通数据块，照常 put、照常覆盖父映射。
                digest = self.blocks.put(data)
                blocks[str(index)] = digest

            with self._chain_lock:
                chain_ids = self._chain_ids_locked()
                if parent_id:
                    if parent_id not in chain_ids:
                        raise BackupError(404, f"父快照 {parent_id} 不在链上")
                    parent = self._load_snapshot(parent_id)
                    if parent.disk_size != disk_size:
                        raise BackupError(
                            400,
                            "增量备份与父快照盘大小不一致："
                            f"{disk_size} != {parent.disk_size}",
                        )
                elif chain_ids:
                    raise BackupError(
                        409,
                        "链已存在，全量备份不能指定空父快照；增量请指定 parent_id",
                    )
                else:
                    parent = None
                    if len(seen) != expected_blocks:
                        raise BackupError(
                            400,
                            f"全量备份必须包含全部 {expected_blocks} 个块，"
                            f"实际收到 {len(seen)} 个",
                        )

            snapshot_id = uuid.uuid4().hex
            record = SnapshotRecord(
                id=snapshot_id,
                parent_id=parent_id,
                disk_size=disk_size,
                block_size=block_size,
                blocks=blocks,
                created_at=datetime.now(timezone.utc).isoformat(),
            )
            with self._chain_lock:
                # 二次确认父快照仍是链尾：单实例线性链只允许向链尾追加。
                if parent_id != self._tail_id_locked():
                    raise BackupError(
                        409,
                        "父快照已不是链尾（可能有并发追加），请基于当前链尾重试",
                    )
                self._write_snapshot(record)
                self.manifest.chain.append(
                    ChainEntry(id=snapshot_id, parent_id=parent_id)
                )
                self._save_manifest_locked()
            return record

    # ------------------------------------------------------------------ 合并

    def merge_snapshot(self, snapshot_id: str):
        """撤下中间快照 snapshot_id，把它的直接子增量改挂到它的父快照。

        只处理这两份增量（被撤快照 S 与其直接子快照 C）涉及的块：
        合并后 C' 在下标 i 上的映射 = S.blocks[i]（若有）再被 C.blocks[i] 覆盖。
        其它快照一个块都不重写。
        """
        with self._merge_lock:
            # ---- 准备阶段：不持链锁，链尾此时仍可追加 ----
            with self._chain_lock:
                idx = self._index_locked(snapshot_id)
                if idx == 0:
                    raise BackupError(400, "链上的第一个（全量）快照不能撤下")
                if idx == len(self.manifest.chain) - 1:
                    raise BackupError(400, "链尾快照没有子增量，无法合并撤下")
                parent_id = self.manifest.chain[idx - 1].id
                child_id = self.manifest.chain[idx + 1].id
                # 取快照副本，锁内读完，准备工作在锁外做。
                snap_s = self._load_snapshot(snapshot_id)
                snap_c = self._load_snapshot(child_id)

            if self.settings.merge_prep_delay:  # 测试钩子
                time.sleep(self.settings.merge_prep_delay)

            merged_blocks = dict(snap_s.blocks)
            merged_blocks.update(snap_c.blocks)  # C 显式覆盖（含显式全零块）优先
            new_child = snap_c.model_copy(
                update={"parent_id": parent_id, "blocks": merged_blocks}
            )

            # ---- 提交阶段：短持链锁，先写新快照 JSON，再原子切换清单 ----
            with self._chain_lock:
                # 重新校验：S/C 的相对位置在准备期间不会因尾追加而改变，
                # 这里防御性确认一遍。
                idx2 = self._index_locked(snapshot_id)
                if (
                    idx2 == 0
                    or idx2 == len(self.manifest.chain) - 1
                    or self.manifest.chain[idx2 - 1].id != parent_id
                    or self.manifest.chain[idx2 + 1].id != child_id
                ):
                    raise BackupError(409, "合并期间链结构发生变化，请重试")
                self._write_snapshot(new_child)
                # 按下标直接构造新链：删掉 S 所在位置，并把 C 的父指针改成 P。
                # 不能先过滤再用旧下标替换，过滤会让后续元素整体左移一位。
                new_chain: list[ChainEntry] = []
                for pos, entry in enumerate(self.manifest.chain):
                    if pos == idx2:
                        continue
                    if entry.id == child_id:
                        new_chain.append(
                            ChainEntry(id=child_id, parent_id=parent_id)
                        )
                    else:
                        new_chain.append(entry)
                old_chain = self.manifest.chain
                self.manifest.chain = new_chain
                try:
                    self._save_manifest_locked()
                except OSError as exc:
                    # 清单没换成：回滚内存状态，旧链在进程内与磁盘上都完整保留。
                    self.manifest.chain = old_chain
                    raise BackupError(500, f"清单写入失败，旧链保留: {exc}") from exc
                # 清单切换成功后才考虑移除被撤快照的元数据；块交给 GC。
                # 若仍有在途恢复 pin 住它（恢复要沿祖先链读这份 JSON），
                # 保留文件，等下次 GC（含重启后的启动 GC）按无引用孤儿清除。
                pinned = self._pinned_snapshot_ids_locked()
                if snapshot_id not in pinned:
                    try:
                        self._snapshot_path(snapshot_id).unlink()
                    except FileNotFoundError:
                        pass
                reclaimed = self.gc_locked()

            return {
                "merged_snapshot_id": child_id,
                "removed_snapshot_id": snapshot_id,
                "new_parent_id": parent_id,
                "touched_block_indices": len(merged_blocks),
                "blocks_reclaimed": reclaimed,
                "chain_after": self._chain_ids_locked(),
            }

    # ------------------------------------------------------------------ 恢复

    def _resolve_index(self, snapshot_id: str, index: int) -> str:
        """沿父链解析某下标的有效块哈希（缺块沿用父快照）。

        正常路径以清单里的父子关系为准；若当前快照已被合并撤下但仍被在途
        恢复 pin 住（不在清单上），回退使用快照 JSON 自带的 parent_id 走完
        剩余祖先链。
        """
        current: Optional[str] = snapshot_id
        guard = 0
        while current is not None:
            snap = self._load_snapshot(current)
            digest = snap.blocks.get(str(index))
            if digest is not None:
                return digest
            with self._chain_lock:
                try:
                    current = self._get_entry_locked(current).parent_id
                except BackupError as exc:
                    if exc.status_code != 404:
                        raise
                    # 快照已不在清单（合并撤下），用 JSON 里的父指针继续。
                    current = snap.parent_id
            guard += 1
            if guard > 1_000_000:
                raise BackupError(500, "快照父子链存在环，无法恢复")
        raise BackupError(500, f"块下标 {index} 在整条祖先链上都没有映射")

    def open_restore_pin(self, snapshot_id: str) -> RestorePin:
        with self._chain_lock:
            self._index_locked(snapshot_id)  # 确认在链上
            pin_id = f"{uuid.uuid4().hex}.pin"
            pin = RestorePin(self.settings.pins_dir, snapshot_id, pin_id)
            with open(self.settings.pins_dir / pin_id, "w", encoding="utf-8") as fh:
                fh.write(snapshot_id)
                fh.flush()
                os.fsync(fh.fileno())
            return pin

    def restore_stream(
        self, snapshot_id: str
    ) -> tuple[Iterator[bytes], int, int, RestorePin]:
        pin = self.open_restore_pin(snapshot_id)
        snap = self._load_snapshot(snapshot_id)
        expected = (
            (snap.disk_size + snap.block_size - 1) // snap.block_size
            if snap.disk_size
            else 0
        )

        def exact_iterator() -> Iterator[bytes]:
            # 逐块沿父链解析；最后一块按盘实际大小截断，
            # 恢复镜像与原盘逐字节等长。pin 在迭代结束或中断时释放。
            remaining = snap.disk_size
            try:
                for index in range(expected):
                    digest = self._resolve_index(snapshot_id, index)
                    need = min(snap.block_size, remaining)
                    leftover = need
                    with self.blocks.open_read(digest) as fh:
                        while leftover > 0:
                            chunk = fh.read(min(1024 * 1024, leftover))
                            if not chunk:
                                raise BackupError(
                                    500, f"块 {digest} 内容短于预期"
                                )
                            yield chunk
                            leftover -= len(chunk)
                    remaining -= need
            finally:
                pin.close()

        return exact_iterator(), snap.disk_size, snap.block_size, pin

    # -------------------------------------------------------------------- GC

    def _pinned_snapshot_ids_locked(self) -> set[str]:
        pinned: set[str] = set()
        for path in self.settings.pins_dir.glob("*.pin"):
            try:
                pinned.add(path.read_text(encoding="utf-8").strip())
            except OSError:
                continue
        return pinned

    def gc_locked(self) -> int:
        """标记-清除：回收无任何保留快照引用、且无在途恢复 pin 的块。"""
        referenced: set[str] = set()
        for entry in self.manifest.chain:
            snap = self._load_snapshot(entry.id)
            referenced.update(snap.blocks.values())
        # pin 住的快照即使正被合并撤下（理论上提交后已不在链上），其块也保留。
        for snapshot_id in self._pinned_snapshot_ids_locked():
            pin_path = self._snapshot_path(snapshot_id)
            if pin_path.exists():
                referenced.update(self._load_snapshot(snapshot_id).blocks.values())
        reclaimed = 0
        for digest in self.blocks.all_hashes():
            if digest not in referenced:
                self.blocks.delete(digest)
                reclaimed += 1
        return reclaimed

    def run_gc(self) -> dict:
        with self._chain_lock:
            chain_ids = set(self._chain_ids_locked())
            pinned_ids = self._pinned_snapshot_ids_locked()
            orphan_files = 0
            for path in self.settings.snapshots_dir.glob("*.json"):
                # 在途恢复 pin 住的已撤快照元数据必须保留到流结束。
                if path.stem not in chain_ids and path.stem not in pinned_ids:
                    path.unlink()
                    orphan_files += 1
            reclaimed = self.gc_locked()
            pins = sorted(pinned_ids)
        return {
            "blocks_reclaimed": reclaimed,
            "orphan_snapshot_files_removed": orphan_files,
            "pinned_snapshots": pins,
        }

    # ----------------------------------------------------------------- 查询

    def list_snapshots(self) -> list[dict]:
        with self._chain_lock:
            tail = self._tail_id_locked()
            result = []
            for entry in self.manifest.chain:
                snap = self._load_snapshot(entry.id)
                result.append(
                    {
                        "id": snap.id,
                        "parent_id": entry.parent_id,
                        "disk_size": snap.disk_size,
                        "block_size": snap.block_size,
                        "block_count": len(snap.blocks),
                        "created_at": snap.created_at,
                        "is_tail": snap.id == tail,
                    }
                )
            return result

    def health(self) -> dict:
        with self._chain_lock:
            return {
                "status": "ok",
                "chain_length": len(self.manifest.chain),
                "blocks_on_disk": len(self.blocks.all_hashes()),
                "active_pins": len(list(self.settings.pins_dir.glob("*.pin"))),
            }
