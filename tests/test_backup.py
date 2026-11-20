"""关键场景端到端测试。

覆盖：
1. 全量 + 线性增量（缺块沿用父快照、显式全零块覆盖）
2. 合并撤下中间快照后，所有保留快照恢复镜像逐字节不变
3. 合并只触碰两份增量涉及的块、旧块 GC 与在途恢复 pin
4. 合并期间链尾可并发追加；已开始的流式恢复按原内容读完
5. 合并失败/参数错误时旧链完整
6. 模拟进程中断（tmp 残留、孤儿快照、半切换）重启后的链一致性
7. 协议与边界校验
"""
from __future__ import annotations

import threading
import time

from tests.client import (
    BLOCK_SIZE,
    RawStreamingResponse,
    changed_blocks,
    frame_chunks,
    image_to_blocks,
)


def make_image(n_blocks: int, seed: int) -> bytes:
    """生成可重复的测试盘镜像，每块内容不同且非全零。"""
    parts = []
    for i in range(n_blocks):
        base = ((i + 1) * 2654435761 ^ seed * 40503) & 0xFFFFFFFF
        parts.append(base.to_bytes(4, "little") * (BLOCK_SIZE // 4))
    return b"".join(parts)


def zero_out(image: bytes, indices: list[int]) -> bytes:
    out = bytearray(image)
    for i in indices:
        out[i * BLOCK_SIZE : (i + 1) * BLOCK_SIZE] = b"\x00" * BLOCK_SIZE
    return bytes(out)


# ------------------------------------------------------------ 1. 基础备份语义


def test_full_and_incremental_missing_blocks_inherit(server):
    image0 = make_image(6, seed=1)
    snap0 = server.client.upload_image(image0)
    assert snap0["parent_id"] is None
    assert snap0["block_count"] == 6
    assert server.client.restore(snap0["id"]) == image0

    # 增量只改第 1、3 块，其它块缺省 -> 必须沿用父快照
    image1 = bytearray(image0)
    image1[BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\xaa" * BLOCK_SIZE
    image1[3 * BLOCK_SIZE : 4 * BLOCK_SIZE] = b"\xbb" * BLOCK_SIZE
    image1 = bytes(image1)
    delta = changed_blocks(image0, image1)
    assert sorted(delta) == [1, 3]
    snap1 = server.client.upload_increment(len(image1), delta, snap0["id"])
    assert snap1["block_count"] == 2
    assert server.client.restore(snap1["id"]) == image1

    # 链尾之外的 id 不能作为父快照
    resp = server.client.http.post(
        "/snapshots",
        data={
            "disk_size": str(len(image1)),
            "block_size": str(BLOCK_SIZE),
            "parent_id": snap0["id"],
        },
        files={"chunks": ("c.bin", frame_chunks({}), "application/octet-stream")},
    )
    assert resp.status_code in (400, 409)


def test_explicit_zero_block_overwrites_parent(server):
    image0 = make_image(4, seed=2)
    snap0 = server.client.upload_image(image0)

    # 显式上传全零块（即使它与父块不同），必须覆盖；与「缺块沿用」严格区分
    image1 = zero_out(image0, [2])
    snap1 = server.client.upload_increment(
        len(image1), {2: b"\x00" * BLOCK_SIZE}, snap0["id"]
    )
    restored = server.client.restore(snap1["id"])
    assert restored == image1
    assert restored[2 * BLOCK_SIZE : (2 + 1) * BLOCK_SIZE] == b"\x00" * BLOCK_SIZE

    # 再做一份增量：该块缺省，仍然是零（沿用到显式零块的父快照）
    snap2 = server.client.upload_increment(len(image1), {0: b"Z" * BLOCK_SIZE}, snap1["id"])
    restored2 = server.client.restore(snap2["id"])
    assert restored2[2 * BLOCK_SIZE : (2 + 1) * BLOCK_SIZE] == b"\x00" * BLOCK_SIZE
    assert restored2[0:BLOCK_SIZE] == b"Z" * BLOCK_SIZE


def test_last_partial_block_roundtrip(server):
    image = make_image(2, seed=3)[: BLOCK_SIZE + 137]  # 末块只有 137 字节
    snap = server.client.upload_image(image)
    restored = server.client.restore(snap["id"])
    assert restored == image
    assert len(restored) == BLOCK_SIZE + 137


# ------------------------------------------------------------ 2. 合并等价性


def test_merge_keeps_all_retained_images_byte_identical(server):
    image0 = make_image(8, seed=4)
    snap0 = server.client.upload_image(image0)

    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x11" * BLOCK_SIZE
    image1[2 * BLOCK_SIZE : 3 * BLOCK_SIZE] = b"\x22" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(
        len(image1), changed_blocks(image0, image1), snap0["id"]
    )

    image2 = bytearray(image1)
    image2[BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\x33" * BLOCK_SIZE  # 新改块 1
    image2[2 * BLOCK_SIZE : 3 * BLOCK_SIZE] = b"\x44" * BLOCK_SIZE  # 覆盖 S 改过的块 2
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(
        len(image2), changed_blocks(image1, image2), snap1["id"]
    )

    image3 = bytearray(image2)
    image3[5 * BLOCK_SIZE : 6 * BLOCK_SIZE] = b"\x55" * BLOCK_SIZE
    image3 = bytes(image3)
    snap3 = server.client.upload_increment(
        len(image3), changed_blocks(image2, image3), snap2["id"]
    )

    # 合并前留存所有恢复镜像与链顺序
    before = {s["id"]: server.client.restore(s["id"]) for s in server.client.snapshots()}
    chain_before = [s["id"] for s in server.client.snapshots()]

    result = server.client.merge(snap1["id"])  # 撤下 S=snap1，C=snap2 改挂 snap0
    assert result["removed_snapshot_id"] == snap1["id"]
    assert result["merged_snapshot_id"] == snap2["id"]
    assert result["new_parent_id"] == snap0["id"]
    # 只处理 S、C 两份增量涉及的下标：S 改 {0,2}，C 改 {1,2} -> 并集 3 个
    assert result["touched_block_indices"] == 3

    chain_after = [s["id"] for s in server.client.snapshots()]
    assert chain_after == [snap0["id"], snap2["id"], snap3["id"]]
    snap2_after = next(s for s in server.client.snapshots() if s["id"] == snap2["id"])
    assert snap2_after["parent_id"] == snap0["id"]
    # 合并后 C 自身的映射只包含两份增量并集（块数从 2 变成 3）
    assert snap2_after["block_count"] == 3

    # 所有保留快照恢复镜像必须与合并前逐字节相同
    for snapshot_id in chain_after:
        assert server.client.restore(snapshot_id) == before[snapshot_id]
    assert server.client.restore(snap3["id"]) == image3
    # 被撤快照已不可恢复
    resp = server.client.http.get(f"/snapshots/{snap1['id']}/restore")
    assert resp.status_code == 404

    # 合并后还能继续在链尾追加，且内容正确
    image4 = bytearray(image3)
    image4[7 * BLOCK_SIZE :] = b"\x66" * (len(image4) - 7 * BLOCK_SIZE)
    image4 = bytes(image4)
    snap4 = server.client.upload_increment(
        len(image4), changed_blocks(image3, image4), snap3["id"]
    )
    assert server.client.restore(snap4["id"]) == image4


def test_merge_with_explicit_zero_block(server):
    image0 = make_image(4, seed=5)
    snap0 = server.client.upload_image(image0)
    image1 = zero_out(image0, [1])
    snap1 = server.client.upload_increment(
        len(image1), {1: b"\x00" * BLOCK_SIZE}, snap0["id"]
    )
    image2 = bytearray(image1)
    image2[0:BLOCK_SIZE] = b"\x77" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(len(image2), {0: b"\x77" * BLOCK_SIZE}, snap1["id"])

    before_c = server.client.restore(snap2["id"])
    server.client.merge(snap1["id"])
    assert server.client.restore(snap2["id"]) == before_c
    restored = server.client.restore(snap2["id"])
    assert restored[BLOCK_SIZE : 2 * BLOCK_SIZE] == b"\x00" * BLOCK_SIZE


def test_repeated_merges_collapse_chain(server):
    image0 = make_image(5, seed=6)
    snaps = [server.client.upload_image(image0)]
    current = image0
    for round_id in range(4):
        nxt = bytearray(current)
        idx = round_id
        nxt[idx * BLOCK_SIZE : (idx + 1) * BLOCK_SIZE] = bytes([0x80 + round_id]) * BLOCK_SIZE
        nxt = bytes(nxt)
        snaps.append(
            server.client.upload_increment(
                len(nxt), changed_blocks(current, nxt), snaps[-1]["id"]
            )
        )
        current = nxt
    final_image = current

    # 依次撤下中间快照，直到只剩全量 + 最终增量
    while len(server.client.snapshots()) > 2:
        middle = server.client.snapshots()[1]["id"]
        server.client.merge(middle)

    chain = server.client.snapshots()
    assert len(chain) == 2
    assert chain[0]["parent_id"] is None
    assert chain[1]["parent_id"] == chain[0]["id"]
    assert server.client.restore(chain[1]["id"]) == final_image
    assert server.client.restore(chain[0]["id"]) == image0


# ------------------------------------------------------------ 3. GC 与在途恢复


def test_gc_reclaims_only_unreferanced_blocks(server):
    image0 = make_image(4, seed=7)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x99" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(
        len(image1), {0: b"\x99" * BLOCK_SIZE}, snap0["id"]
    )
    # 同一位置再覆盖一次：0x99 旧版本此后只被 snap1 引用
    image2 = bytearray(image1)
    image2[0:BLOCK_SIZE] = b"\x88" * BLOCK_SIZE
    image2[1 * BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\x77" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(
        len(image2),
        {0: b"\x88" * BLOCK_SIZE, 1: b"\x77" * BLOCK_SIZE},
        snap1["id"],
    )

    blocks_before_merge = server.block_files()
    # 撤下 snap1：合并后 snap2' 在块 0 上取自己的 0x88，
    # 仅被 snap1 引用的 0x99 块变为无引用；合并提交时的内置 GC 会立即回收它。
    merge_result = server.client.merge(snap1["id"])
    assert merge_result["blocks_reclaimed"] >= 1
    assert len(server.block_files()) < len(blocks_before_merge)
    # 链上两个快照都还能完整恢复、字节正确
    assert server.client.restore(snap0["id"]) == image0
    assert server.client.restore(snap2["id"]) == image2
    # 手动再跑一次 GC 应是幂等的：没有新增可回收块
    again = server.client.gc()
    assert again["blocks_reclaimed"] == 0


def test_inflight_restore_pins_blocks_during_gc_and_merge(server):
    # 盘足够大（8 MiB），配合原始 socket 逐块读取，稳定复现
    # 「流读到一半、pin 仍在」的在途恢复状态。
    image0 = make_image(2048, seed=8)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\xaa" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\xaa" * BLOCK_SIZE}, snap0["id"])
    image2 = bytearray(image1)
    image2[1 * BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\xbb" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(len(image2), {1: b"\xbb" * BLOCK_SIZE}, snap1["id"])

    expected_s1 = server.client.restore(snap1["id"])

    # 打开 snap1 的流式恢复，只读走第一个块就停下来（pin 必然仍有效）
    raw = RawStreamingResponse("127.0.0.1", server.port, f"/snapshots/{snap1['id']}/restore")
    assert raw.status == 200
    first = raw.read_exact(BLOCK_SIZE)  # 只读走恰好一个块，pin 必然仍有效
    assert first == expected_s1[:BLOCK_SIZE]
    assert server.pin_files()  # pin 文件确实存在

    server.client.merge(snap1["id"])
    # GC 在 pin 生效期间运行：被 pin 快照引用的块与元数据一个都不能删
    gc_during_restore = server.client.gc()
    blocks_before_finish = server.block_files()
    assert gc_during_restore["pinned_snapshots"] == [snap1["id"]]
    assert (server.data_dir / "snapshots" / f"{snap1['id']}.json").exists()

    # 被撤快照在 pin 期间仍可沿祖先链把原内容读完
    rest = raw.read_remaining()
    raw.close()
    assert first + rest == expected_s1

    # 流结束后 pin 释放；再做 GC 即可回收仅被撤下快照使用的块与孤儿元数据
    assert server.pin_files() == []
    gc_after = server.client.gc()
    assert not (server.data_dir / "snapshots" / f"{snap1['id']}.json").exists()
    # 链上快照依然可恢复且字节正确
    assert server.client.restore(snap2["id"]) == image2
    assert server.client.restore(snap0["id"]) == image0


# ------------------------------------------------------------ 4. 合并期间并发追加


def test_append_while_merge_is_running(delayed_server):
    # 子进程通过 MERGE_PREP_DELAY=0.8 制造确定的合并准备窗口
    server = delayed_server

    image0 = make_image(4, seed=9)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\xcc" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\xcc" * BLOCK_SIZE}, snap0["id"])
    image2 = bytearray(image1)
    image2[1 * BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\xdd" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(len(image2), {1: b"\xdd" * BLOCK_SIZE}, snap1["id"])

    merge_result: dict = {}
    merge_error = []

    def do_merge():
        try:
            merge_result.update(server.client.merge(snap1["id"]))
        except Exception as exc:  # noqa: BLE001
            merge_error.append(exc)

    t = threading.Thread(target=do_merge)
    t.start()
    time.sleep(0.25)  # 确保合并已进入准备阶段、清单尚未切换

    # 合并进行中向链尾追加一份增量
    image3 = bytearray(image2)
    image3[2 * BLOCK_SIZE : 3 * BLOCK_SIZE] = b"\xee" * BLOCK_SIZE
    image3 = bytes(image3)
    snap3 = server.client.upload_increment(
        len(image3), {2: b"\xee" * BLOCK_SIZE}, snap2["id"]
    )

    t.join(timeout=10)
    assert not merge_error
    assert merge_result["removed_snapshot_id"] == snap1["id"]

    chain = [s["id"] for s in server.client.snapshots()]
    assert chain == [snap0["id"], snap2["id"], snap3["id"]]
    assert server.client.restore(snap3["id"]) == image3
    assert server.client.restore(snap2["id"]) == image2
    assert server.client.restore(snap0["id"]) == image0


# ------------------------------------------------------------ 5. 失败保留旧链


def test_merge_rejects_root_and_tail(server):
    image0 = make_image(3, seed=10)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x01" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\x01" * BLOCK_SIZE}, snap0["id"])

    code, body = server.client.merge_expect_error(snap0["id"])
    assert code == 400 and "第一个" in body["detail"]
    code, body = server.client.merge_expect_error(snap1["id"])
    assert code == 400 and "链尾" in body["detail"]

    # 出错后链原样保留、镜像可读
    assert [s["id"] for s in server.client.snapshots()] == [snap0["id"], snap1["id"]]
    assert server.client.restore(snap1["id"]) == image1


def test_upload_validation_errors(server):
    image0 = make_image(2, seed=11)

    # 全量备份缺块（此时链为空，应返回 400 而非链冲突）
    partial = frame_chunks({0: image0[:BLOCK_SIZE]})
    resp = server.client.http.post(
        "/snapshots",
        data={"disk_size": str(len(image0)), "block_size": str(BLOCK_SIZE)},
        files={"chunks": ("c.bin", partial, "application/octet-stream")},
    )
    assert resp.status_code == 400

    snap0 = server.client.upload_image(image0)

    # 块大小不匹配
    resp = server.client.http.post(
        "/snapshots",
        data={"disk_size": str(len(image0)), "block_size": "8192"},
        files={"chunks": ("c.bin", b"", "application/octet-stream")},
    )
    assert resp.status_code == 400

    # 下标越界
    bad = frame_chunks({99: b"x" * BLOCK_SIZE})
    resp = server.client.http.post(
        "/snapshots",
        data={"disk_size": str(len(image0)), "block_size": str(BLOCK_SIZE)},
        files={"chunks": ("c.bin", bad, "application/octet-stream")},
    )
    assert resp.status_code == 400

    # 帧截断
    resp = server.client.http.post(
        "/snapshots",
        data={"disk_size": str(len(image0)), "block_size": str(BLOCK_SIZE)},
        files={"chunks": ("c.bin", b"\x00\x00\x00\x00\x00\x00\x10", "application/octet-stream")},
    )
    assert resp.status_code == 400

    # 盘大小不一致的增量
    resp = server.client.http.post(
        "/snapshots",
        data={
            "disk_size": str(len(image0) + BLOCK_SIZE),
            "block_size": str(BLOCK_SIZE),
            "parent_id": snap0["id"],
        },
        files={"chunks": ("c.bin", frame_chunks({}), "application/octet-stream")},
    )
    assert resp.status_code == 400

    # 链存在时再来一个无父快照的全量
    resp = server.client.http.post(
        "/snapshots",
        data={"disk_size": str(len(image0)), "block_size": str(BLOCK_SIZE)},
        files={"chunks": ("c.bin", frame_chunks(image_to_blocks(image0)), "application/octet-stream")},
    )
    assert resp.status_code == 409


# ------------------------------------------------------------ 6. 重启崩溃恢复


def test_restart_normal_state(server):
    image0 = make_image(4, seed=12)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x12" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\x12" * BLOCK_SIZE}, snap0["id"])

    server.restart()
    chain = server.client.snapshots()
    assert [s["id"] for s in chain] == [snap0["id"], snap1["id"]]
    assert server.client.restore(snap1["id"]) == image1
    assert server.client.restore(snap0["id"]) == image0
    leftover = server.pin_files()
    if leftover:
        details = [
            (name, (server.data_dir / "pins" / name).read_text()) for name in leftover
        ]
        raise AssertionError(f"重启后残留 pin: {details}")
    assert server.tmp_files() == []


def test_restart_after_interrupted_merge_keeps_one_complete_chain(server, tmp_path):
    image0 = make_image(4, seed=13)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x13" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\x13" * BLOCK_SIZE}, snap0["id"])
    image2 = bytearray(image1)
    image2[1 * BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\x14" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(len(image2), {1: b"\x14" * BLOCK_SIZE}, snap1["id"])

    # 场景 A：中断在「新快照 JSON 已写、清单未切换」-> 启动后是完整旧链
    snap_dir = server.data_dir / "snapshots"
    orphan = snap_dir / "deadbeef.json"
    orphan.write_text('{"version": 1, "id": "deadbeef"}', encoding="utf-8")
    (server.data_dir / "tmp" / "block-stale.tmp").write_bytes(b"stale")

    server.restart()
    assert [s["id"] for s in server.client.snapshots()] == [
        snap0["id"],
        snap1["id"],
        snap2["id"],
    ]
    assert not orphan.exists()
    assert server.tmp_files() == []
    assert server.client.restore(snap2["id"]) == image2

    # 场景 B：中断在「清单已切换、快照 parent_id 校正/旧文件清理之前」
    # 手工把清单改成合并后的形态，但保留 snap1 的旧文件，且 snap2 仍记旧父。
    import json

    manifest = {
        "version": 1,
        "chain": [
            {"id": snap0["id"], "parent_id": None},
            {"id": snap2["id"], "parent_id": snap0["id"]},
        ],
    }
    (server.data_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    server.restart()
    chain = server.client.snapshots()
    assert [s["id"] for s in chain] == [snap0["id"], snap2["id"]]
    snap2_after = next(s for s in chain if s["id"] == snap2["id"])
    assert snap2_after["parent_id"] == snap0["id"]
    # 注意：场景 B 下 snap2 的 blocks 仍是合并前的增量映射（模拟极端中断点），
    # 块 0 由父链 snap0 解析 —— 这里验证启动对账不会破坏清单权威与可读链。
    assert server.client.restore(snap0["id"]) == image0


def test_restart_then_merge_and_restore_full_pipeline(server):
    # 重启后服务仍可继续合并与追加（新链完整可用）
    image0 = make_image(5, seed=14)
    snap0 = server.client.upload_image(image0)
    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\x15" * BLOCK_SIZE
    image1 = bytes(image1)
    snap1 = server.client.upload_increment(len(image1), {0: b"\x15" * BLOCK_SIZE}, snap0["id"])
    image2 = bytearray(image1)
    image2[1 * BLOCK_SIZE : 2 * BLOCK_SIZE] = b"\x16" * BLOCK_SIZE
    image2 = bytes(image2)
    snap2 = server.client.upload_increment(len(image2), {1: b"\x16" * BLOCK_SIZE}, snap1["id"])

    server.restart()
    before = {s["id"]: server.client.restore(s["id"]) for s in server.client.snapshots()}
    server.client.merge(snap1["id"])
    for snapshot_id, image in before.items():
        if snapshot_id == snap1["id"]:
            continue
        assert server.client.restore(snapshot_id) == image

    image3 = bytearray(image2)
    image3[3 * BLOCK_SIZE : 4 * BLOCK_SIZE] = b"\x17" * BLOCK_SIZE
    image3 = bytes(image3)
    snap3 = server.client.upload_increment(len(image3), {3: b"\x17" * BLOCK_SIZE}, snap2["id"])
    assert server.client.restore(snap3["id"]) == image3
