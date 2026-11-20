"""服务运行配置。

所有路径都通过环境变量覆盖，Docker Compose 中把 DATA_DIR 挂到持久卷。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须为正整数，实际为 {value}")
    return value


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    block_size: int
    # 测试钩子：合并准备阶段（新元数据落盘、切换清单之前）的额外延迟秒数，
    # 生产环境保持 0。
    merge_prep_delay: float

    @property
    def blocks_dir(self) -> Path:
        return self.data_dir / "blocks"

    @property
    def snapshots_dir(self) -> Path:
        return self.data_dir / "snapshots"

    @property
    def pins_dir(self) -> Path:
        return self.data_dir / "pins"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def manifest_path(self) -> Path:
        return self.data_dir / "manifest.json"


def get_settings() -> Settings:
    block_size = _env_int("BLOCK_SIZE", 4096)
    delay = float(os.environ.get("MERGE_PREP_DELAY", "0") or 0)
    return Settings(
        data_dir=Path(os.environ.get("DATA_DIR", "/data")),
        block_size=block_size,
        merge_prep_delay=delay,
    )
