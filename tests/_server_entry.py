"""测试服务子进程入口：与容器里相同的 uvicorn 启动方式。"""
from __future__ import annotations

import uvicorn

from app.main import app

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(__import__("os").environ["TEST_PORT"]), log_level="warning")
