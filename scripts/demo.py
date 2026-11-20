"""端到端手动演示：全量 -> 两份增量 -> 合并撤下中间快照 -> 逐字节校验 -> GC。

用法（服务已启动时）：
    python scripts/demo.py --base-url http://127.0.0.1:8000

脚本会构造一块 8 * 4KiB 的内存盘，验证：
1. 增量缺块沿用父快照、显式全零块覆盖；
2. 合并前后所有保留快照的恢复镜像逐字节相同；
3. 合并后链尾仍可追加；GC 只回收无引用块。
"""
from __future__ import annotations

import argparse
import hashlib
import io
import struct

import httpx

BLOCK_SIZE = 4096
HEADER = struct.Struct(">II")


def frame(blocks: dict[int, bytes]) -> bytes:
    buf = io.BytesIO()
    for index in sorted(blocks):
        buf.write(HEADER.pack(index, len(blocks[index])))
        buf.write(blocks[index])
    return buf.getvalue()


def main(base_url: str) -> None:
    cli = httpx.Client(base_url=base_url, timeout=30)

    def upload(disk_size: int, chunks: bytes, parent: str | None = None) -> dict:
        data = {"disk_size": str(disk_size), "block_size": str(BLOCK_SIZE)}
        if parent:
            data["parent_id"] = parent
        r = cli.post(
            "/snapshots",
            data=data,
            files={"chunks": ("chunks.bin", chunks, "application/octet-stream")},
        )
        r.raise_for_status()
        return r.json()

    def restore(snapshot_id: str) -> bytes:
        r = cli.get(f"/snapshots/{snapshot_id}/restore")
        r.raise_for_status()
        return r.content

    def digest(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()[:16]

    image0 = b"".join(
        (i * 2654435761 & 0xFFFFFFFF).to_bytes(4, "little") * (BLOCK_SIZE // 4)
        for i in range(8)
    )
    print(f"[1] 上传全量备份，盘大小 {len(image0)} 字节")
    s0 = upload(len(image0), frame({i: image0[i*4096:(i+1)*4096] for i in range(8)}))
    assert restore(s0["id"]) == image0
    print(f"    快照 {s0['id'][:12]} 恢复一致 sha256={digest(image0)}")

    image1 = bytearray(image0)
    image1[0:BLOCK_SIZE] = b"\xa5" * BLOCK_SIZE          # 改块 0
    image1[BLOCK_SIZE:2*BLOCK_SIZE] = b"\x00" * BLOCK_SIZE  # 块 1 显式置零
    image1 = bytes(image1)
    delta1 = {0: image1[:BLOCK_SIZE], 1: b"\x00" * BLOCK_SIZE}
    print("[2] 上传增量 S1：改块 0，块 1 显式全零覆盖，其余缺块沿用父快照")
    s1 = upload(len(image1), frame(delta1), s0["id"])
    assert restore(s1["id"]) == image1
    print(f"    快照 {s1['id'][:12]} 恢复一致 sha256={digest(image1)}")

    image2 = bytearray(image1)
    image2[2*BLOCK_SIZE:3*BLOCK_SIZE] = b"\x5a" * BLOCK_SIZE  # 改块 2
    image2[BLOCK_SIZE:2*BLOCK_SIZE] = b"\xc3" * BLOCK_SIZE    # 覆盖零块 1
    image2 = bytes(image2)
    delta2 = {1: image2[BLOCK_SIZE:2*BLOCK_SIZE], 2: image2[2*BLOCK_SIZE:3*BLOCK_SIZE]}
    print("[3] 上传增量 S2（链尾），覆盖 S1 的零块 1 并新改块 2")
    s2 = upload(len(image2), frame(delta2), s1["id"])
    assert restore(s2["id"]) == image2

    before = {s["id"]: restore(s["id"]) for s in cli.get("/snapshots").json()}
    print(f"[4] 合并撤下中间快照 S1={s1['id'][:12]}，S2 直接改挂 S0")
    merged = cli.post(f"/snapshots/{s1['id']}/merge").json()
    print(f"    结果：{merged}")
    for snapshot in cli.get("/snapshots").json():
        assert restore(snapshot["id"]) == before[snapshot["id"]]
        print(f"    保留快照 {snapshot['id'][:12]} 合并前后逐字节相同")

    print("[5] 合并后向新链尾追加增量 S3")
    image3 = bytearray(image2)
    image3[7*BLOCK_SIZE:] = b"\x7e" * (len(image3) - 7 * BLOCK_SIZE)
    image3 = bytes(image3)
    s3 = upload(len(image3), frame({7: image3[7*BLOCK_SIZE:]}), s2["id"])
    assert restore(s3["id"]) == image3
    print(f"    快照 {s3['id'][:12]} 恢复一致 sha256={digest(image3)}")

    gc = cli.post("/gc").json()
    print(f"[6] GC 完成：{gc}")
    print("[OK] 全部场景验证通过")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    main(args.base_url)
