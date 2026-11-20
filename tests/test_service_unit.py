"""不经过 HTTP 的服务层单元测试：故障注入与原子性。

与端到端子进程测试互补，这里直接在进程内构造 BackupService，
用 monkeypatch 注入磁盘写失败，验证合并失败时旧链完整保留。
"""
from __future__ import annotations

import pytest

from app.config import Settings
from app.service import BackupError, BackupService
from tests.client import BLOCK_SIZE, frame_chunks
import io


def _settings(tmp_path) -> Settings:
    return Settings(
        data_dir=tmp_path,
        block_size=BLOCK_SIZE,
        merge_prep_delay=0.0,
    )


def _make_service(tmp_path) -> BackupService:
    return BackupService(_settings(tmp_path))


def _upload(service: BackupService, disk_size: int, blocks, parent=None):
    return service.create_snapshot(
        parent_id=parent,
        disk_size=disk_size,
        block_size=BLOCK_SIZE,
        chunk_stream=io.BytesIO(frame_chunks(blocks)),
    )


def test_merge_manifest_write_failure_keeps_old_chain(tmp_path, monkeypatch):
    service = _make_service(tmp_path)
    image = bytes((i % 251 for i in range(BLOCK_SIZE * 4)))
    s0 = _upload(
        service,
        len(image),
        {i: image[i * BLOCK_SIZE : (i + 1) * BLOCK_SIZE] for i in range(4)},
    )
    image1 = bytearray(image)
    image1[0:BLOCK_SIZE] = b"\x01" * BLOCK_SIZE
    image1 = bytes(image1)
    image2 = bytearray(image1)
    image2[BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\x02" * BLOCK_SIZE
    image2 = bytes(image2)
    s1 = _upload(service, len(image1), {0: image1[:BLOCK_SIZE]}, s0.id)
    s2 = _upload(
        service,
        len(image2),
        {1: image2[BLOCK_SIZE : 2 * BLOCK_SIZE]},
        s1.id,
    )
    chain_before = [e.id for e in service.manifest.chain]

    real_save = service._save_manifest_locked

    def failing_save():
        raise OSError("模拟清单磁盘写失败")

    monkeypatch.setattr(service, "_save_manifest_locked", failing_save)
    with pytest.raises(BackupError) as exc:
        service.merge_snapshot(s1.id)
    assert exc.value.status_code == 500
    monkeypatch.setattr(service, "_save_manifest_locked", real_save)

    # 内存链回滚到旧链
    assert [e.id for e in service.manifest.chain] == chain_before
    # 磁盘清单仍是旧链
    from app.storage import read_json

    on_disk = read_json(tmp_path / "manifest.json")
    assert [e["id"] for e in on_disk["chain"]] == chain_before
    # 旧链上三个快照都能按原内容恢复
    for snap, expected in ((s0, image), (s1, image1), (s2, image2)):
        iterator, _, _, pin = service.restore_stream(snap.id)
        assert b"".join(iterator) == expected
        pin.close()
    # 故障排除后可正常重试合并
    result = service.merge_snapshot(s1.id)
    assert result["removed_snapshot_id"] == s1.id
    assert [e.id for e in service.manifest.chain] == [s0.id, s2.id]


def test_atomic_write_leaves_no_tmp_on_failure(tmp_path):
    service = _make_service(tmp_path)
    # 正常写入不留 tmp
    s0 = _upload(
        service,
        BLOCK_SIZE,
        {0: b"x" * BLOCK_SIZE},
    )
    assert list((tmp_path / "tmp").iterdir()) == []
    assert s0.blocks["0"]


def test_block_length_validation(tmp_path):
    service = _make_service(tmp_path)
    # 非末块短块 -> 400
    with pytest.raises(BackupError) as exc:
        _upload(service, BLOCK_SIZE * 2, {0: b"short", 1: b"y" * BLOCK_SIZE})
    assert exc.value.status_code == 400 and "长度" in exc.value.detail
    # 末块允许不足 block_size
    snap = _upload(service, BLOCK_SIZE + 10, {0: b"z" * BLOCK_SIZE, 1: b"t" * 10})
    iterator, disk_size, _, pin = service.restore_stream(snap.id)
    assert disk_size == BLOCK_SIZE + 10
    assert b"".join(iterator) == b"z" * BLOCK_SIZE + b"t" * 10
    pin.close()
    # 末块却给了整块（超出 disk_size）-> 400
    with pytest.raises(BackupError) as exc2:
        _upload(service, BLOCK_SIZE + 10, {0: b"z" * BLOCK_SIZE, 1: b"t" * BLOCK_SIZE})
    assert exc2.value.status_code == 400


def test_restart_clears_stale_tmp_and_orphan_snapshot(tmp_path):
    service = _make_service(tmp_path)
    s0 = _upload(service, BLOCK_SIZE, {0: b"x" * BLOCK_SIZE})
    # 注入崩溃残留
    (tmp_path / "tmp" / "stale.tmp").write_bytes(b"stale")
    (tmp_path / "snapshots" / "orphan.json").write_text("{}", encoding="utf-8")
    (tmp_path / "pins" / "old.pin").write_text(s0.id, encoding="utf-8")

    BackupService(_settings(tmp_path))  # 模拟重启
    assert not (tmp_path / "tmp" / "stale.tmp").exists()
    assert not (tmp_path / "snapshots" / "orphan.json").exists()
    assert list((tmp_path / "pins").iterdir()) == []
    # 链仍可读
    again = BackupService(_settings(tmp_path))
    assert [e.id for e in again.manifest.chain] == [s0.id]
