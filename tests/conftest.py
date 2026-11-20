"""pytest 夹具：用独立子进程跑真实 uvicorn 服务，并支持真正的「重启」。

选择子进程而非线程的原因：anyio 的默认工作线程池是进程级全局资源，
在同一进程里反复启停事件循环会让旧 StreamingResponse 生成器的收尾
（释放恢复 pin）与新实例的启动清理产生竞态；真实部署的「进程中断重启」
本来就是进程级隔离。子进程 kill + 重启与生产语义完全一致，
DATA_DIR 指向同一个目录即可验证崩溃恢复。
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# 测试文件会导入 app.main（导入即构造服务单例），先给一个可写的默认 DATA_DIR；
# 每个测试夹具仍会用独立 tmp 目录在子进程里重新构造服务。
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="backup-import-"))
os.environ.setdefault("MERGE_PREP_DELAY", "0")

from tests.client import BackupClient  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ServerHandle:
    def __init__(self, data_dir: Path, port: int, merge_delay: float) -> None:
        self.data_dir = data_dir
        self.port = port
        self.merge_delay = merge_delay
        self.client = BackupClient(f"http://127.0.0.1:{port}")
        self._proc: subprocess.Popen | None = None

    def _spawn(self) -> None:
        env = os.environ.copy()
        env["DATA_DIR"] = str(self.data_dir)
        env["BLOCK_SIZE"] = "4096"
        env["MERGE_PREP_DELAY"] = str(self.merge_delay)
        env["TEST_PORT"] = str(self.port)
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        self._proc = subprocess.Popen(
            [sys.executable, "-m", "tests._server_entry"],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def start(self) -> None:
        self._spawn()
        deadline = time.time() + 20
        last_exc: Exception | None = None
        while time.time() < deadline:
            if self._proc and self._proc.poll() is not None:
                raise RuntimeError(f"测试服务进程提前退出，code={self._proc.returncode}")
            try:
                self.health()
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                time.sleep(0.05)
        raise RuntimeError(f"测试服务启动超时: {last_exc!r}")

    def stop(self) -> None:
        if self._proc is None:
            return
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
        self._proc = None

    def restart(self) -> None:
        """真正的进程中断重启：终止旧进程，同 DATA_DIR 起新进程。"""
        self.stop()
        self._spawn()
        deadline = time.time() + 20
        last_exc: Exception | None = None
        while time.time() < deadline:
            if self._proc and self._proc.poll() is not None:
                raise RuntimeError(f"重启后服务进程提前退出，code={self._proc.returncode}")
            try:
                self.health()
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                time.sleep(0.05)
        raise RuntimeError(f"重启后测试服务启动超时: {last_exc!r}")

    def health(self) -> dict:
        return self.client.health()

    def block_files(self) -> set[str]:
        root = self.data_dir / "blocks"
        out = set()
        for shard in root.iterdir():
            if shard.is_dir():
                out.update(p.name for p in shard.iterdir() if p.is_file())
        return out

    def pin_files(self) -> list[str]:
        return sorted(p.name for p in (self.data_dir / "pins").iterdir())

    def tmp_files(self) -> list[str]:
        return sorted(p.name for p in (self.data_dir / "tmp").iterdir())


@pytest.fixture()
def server(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    port = _free_port()
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    merge_delay = float(os.environ.get("TEST_MERGE_DELAY", "0"))

    handle = ServerHandle(data_dir, port, merge_delay)
    handle.start()
    try:
        yield handle
    finally:
        handle.client.close()
        handle.stop()


@pytest.fixture()
def delayed_server(tmp_path, monkeypatch):
    """合并准备阶段带 0.8s 延迟的服务，用于确定性测试合并期间的并发追加。"""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    port = _free_port()
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    handle = ServerHandle(data_dir, port, merge_delay=0.8)
    handle.start()
    try:
        yield handle
    finally:
        handle.client.close()
        handle.stop()
