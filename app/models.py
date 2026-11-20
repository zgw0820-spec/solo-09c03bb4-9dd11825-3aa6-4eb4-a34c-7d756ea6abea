"""Pydantic 模型与快照/清单的数据结构。"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

MANIFEST_VERSION = 1
SNAPSHOT_VERSION = 1


class SnapshotRecord(BaseModel):
    """落盘的快照元数据。blocks 的键是块下标（字符串形式的整数）。"""

    version: int = SNAPSHOT_VERSION
    id: str
    parent_id: Optional[str] = None
    disk_size: int = Field(..., ge=0)
    block_size: int = Field(..., gt=0)
    blocks: dict[str, str]
    created_at: str


class ChainEntry(BaseModel):
    """清单中的链节点。清单是父子顺序的唯一权威。"""

    id: str
    parent_id: Optional[str] = None


class Manifest(BaseModel):
    version: int = MANIFEST_VERSION
    chain: list[ChainEntry]


class SnapshotOut(BaseModel):
    id: str
    parent_id: Optional[str] = None
    disk_size: int
    block_size: int
    block_count: int
    created_at: str
    is_tail: bool


class MergeResult(BaseModel):
    merged_snapshot_id: str
    removed_snapshot_id: str
    new_parent_id: Optional[str]
    touched_block_indices: int
    blocks_reclaimed: int
    chain_after: list[str]


class GcResult(BaseModel):
    blocks_reclaimed: int
    orphan_snapshot_files_removed: int
    pinned_snapshots: list[str]


class HealthOut(BaseModel):
    status: str
    chain_length: int
    blocks_on_disk: int
    active_pins: int
