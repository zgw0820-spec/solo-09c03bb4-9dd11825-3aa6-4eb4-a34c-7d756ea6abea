"""测试用 HTTP 客户端与分块帧工具。"""
from __future__ import annotations

import io
import socket
import struct

import httpx

CHUNK_HEADER = struct.Struct(">II")
BLOCK_SIZE = 4096


class RawStreamingResponse:
    """用原始 socket 读响应，避免 httpx 对小响应的预缓冲，精确模拟在途恢复。

    每次 read 只从内核缓冲区取当前可用字节，服务端此时必然还没收到流结束。
    """

    def __init__(self, host: str, port: int, path: str) -> None:
        self.sock = socket.create_connection((host, port), timeout=10)
        self.sock.sendall(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise RuntimeError("连接在响应头结束前关闭")
            buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        self.status = int(head.split(b"\r\n", 1)[0].split()[1])
        self._leftover = rest

    def read_exact(self, size: int) -> bytes:
        """可靠读满恰好 size 字节（跨多次 recv 拼接），保证在途状态可控。"""
        parts = []
        remaining = size
        if self._leftover:
            take = self._leftover[:remaining]
            parts.append(take)
            self._leftover = self._leftover[len(take) :]
            remaining -= len(take)
        while remaining > 0:
            chunk = self.sock.recv(remaining)
            if not chunk:
                raise RuntimeError("连接提前关闭")
            parts.append(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    def read_some(self, size: int = 4096) -> bytes:
        if self._leftover:
            data, self._leftover = self._leftover[:size], self._leftover[size:]
            return data
        return self.sock.recv(size)

    def read_remaining(self) -> bytes:
        chunks = [self._leftover]
        self._leftover = b""
        while True:
            chunk = self.sock.recv(1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        self.sock.close()
        return b"".join(chunks)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def frame_chunks(blocks: dict[int, bytes]) -> bytes:
    """把 {下标: 数据} 编码成上传用的分块帧字节流。"""
    buf = io.BytesIO()
    for index in sorted(blocks):
        data = blocks[index]
        buf.write(CHUNK_HEADER.pack(index, len(data)))
        buf.write(data)
    return buf.getvalue()


def image_to_blocks(image: bytes, block_size: int = BLOCK_SIZE) -> dict[int, bytes]:
    blocks: dict[int, bytes] = {}
    for index in range(0, len(image), block_size):
        blocks[index // block_size] = image[index : index + block_size]
    return blocks


def changed_blocks(old: bytes, new: bytes, block_size: int = BLOCK_SIZE) -> dict[int, bytes]:
    """仅保留发生变化的块，模拟线性增量备份。"""
    assert len(old) == len(new)
    out: dict[int, bytes] = {}
    for index, block in image_to_blocks(new, block_size).items():
        if block != old[index * block_size : (index + 1) * block_size]:
            out[index] = block
    return out


class BackupClient:
    def __init__(self, base_url: str) -> None:
        self.http = httpx.Client(base_url=base_url, timeout=30)

    def close(self) -> None:
        self.http.close()

    def upload(
        self,
        disk_size: int,
        chunks: bytes,
        parent_id: str | None = None,
        block_size: int = BLOCK_SIZE,
    ) -> dict:
        files = {"chunks": ("chunks.bin", chunks, "application/octet-stream")}
        data = {"disk_size": str(disk_size), "block_size": str(block_size)}
        if parent_id is not None:
            data["parent_id"] = parent_id
        resp = self.http.post("/snapshots", data=data, files=files)
        assert resp.status_code == 201, resp.text
        return resp.json()

    def upload_image(
        self, image: bytes, parent_id: str | None = None
    ) -> dict:
        return self.upload(
            len(image), frame_chunks(image_to_blocks(image)), parent_id
        )

    def upload_increment(
        self, disk_size: int, blocks: dict[int, bytes], parent_id: str
    ) -> dict:
        return self.upload(disk_size, frame_chunks(blocks), parent_id)

    def snapshots(self) -> list[dict]:
        resp = self.http.get("/snapshots")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def merge(self, snapshot_id: str) -> dict:
        resp = self.http.post(f"/snapshots/{snapshot_id}/merge")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def merge_expect_error(self, snapshot_id: str) -> tuple[int, dict]:
        resp = self.http.post(f"/snapshots/{snapshot_id}/merge")
        return resp.status_code, resp.json()

    def restore(self, snapshot_id: str) -> bytes:
        resp = self.http.get(f"/snapshots/{snapshot_id}/restore")
        assert resp.status_code == 200, resp.text
        return resp.content

    def restore_stream(self, snapshot_id: str):
        return self.http.stream(
            "GET", f"/snapshots/{snapshot_id}/restore", timeout=30
        )

    def gc(self) -> dict:
        resp = self.http.post("/gc")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def health(self) -> dict:
        resp = self.http.get("/health")
        assert resp.status_code == 200, resp.text
        return resp.json()
