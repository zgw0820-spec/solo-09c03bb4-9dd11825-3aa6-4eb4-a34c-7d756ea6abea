"""FastAPI 路由：上传分块快照、列出/合并快照、流式恢复镜像、GC、健康检查。

交互方式（Swagger UI /docs 可直接操作）：
* POST /snapshots        multipart 表单上传一份备份
* GET  /snapshots        查看链上全部快照
* POST /snapshots/{id}/merge  撤下指定中间快照并合并到其子增量
* GET  /snapshots/{id}/restore  下载恢复出的原始盘镜像（字节流）
* POST /gc               手动触发块回收
* GET  /health           健康检查

服务单例挂在 ``app.state.service`` 上，测试模拟「进程重启」时替换该实例即可，
路由通过依赖 ``get_service`` 每次取当前实例。
"""
from __future__ import annotations

import io
from typing import Optional

from fastapi import Depends, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from .config import get_settings
from .models import GcResult, HealthOut, MergeResult, SnapshotOut
from .service import BackupError, BackupService

app = FastAPI(
    title="单实例分块备份服务",
    version="1.0.0",
    description=(
        "接收 4 KiB 分块的单盘全量备份与线性增量备份，支持中间快照合并撤下、"
        "流式恢复与引用计数块回收。分块帧格式：`index(uint32 大端) + "
        "length(uint32 大端) + length 字节数据`，未上传的下标视为沿用父快照，"
        "显式上传的全零块照常覆盖。"
    ),
)
app.state.service = BackupService(get_settings())


def get_service(request: Request) -> BackupService:
    return request.app.state.service


@app.exception_handler(BackupError)
async def backup_error_handler(_: Request, exc: BackupError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.post(
    "/snapshots",
    response_model=SnapshotOut,
    status_code=201,
    summary="上传一份全量或增量备份",
)
async def create_snapshot(
    service: BackupService = Depends(get_service),
    disk_size: int = Form(..., description="盘镜像总字节数"),
    block_size: int = Form(..., description="分块大小，固定 4096"),
    parent_id: Optional[str] = Form(
        None, description="父快照 id；全量备份留空，增量必须是当前链尾"
    ),
    chunks: UploadFile = File(
        ..., description="分块帧序列：index(u32BE)+length(u32BE)+data，可重复"
    ),
):
    raw = await chunks.read()
    record = service.create_snapshot(
        parent_id=parent_id,
        disk_size=disk_size,
        block_size=block_size,
        chunk_stream=io.BytesIO(raw),
    )
    chain = service.list_snapshots()
    return SnapshotOut(
        id=record.id,
        parent_id=record.parent_id,
        disk_size=record.disk_size,
        block_size=record.block_size,
        block_count=len(record.blocks),
        created_at=record.created_at,
        is_tail=any(item["id"] == record.id and item["is_tail"] for item in chain),
    )


@app.get("/snapshots", response_model=list[SnapshotOut], summary="列出链上全部快照")
def list_snapshots(service: BackupService = Depends(get_service)):
    return service.list_snapshots()


@app.post(
    "/snapshots/{snapshot_id}/merge",
    response_model=MergeResult,
    summary="合并并撤下指定的中间快照",
)
def merge_snapshot(snapshot_id: str, service: BackupService = Depends(get_service)):
    return service.merge_snapshot(snapshot_id)


@app.get(
    "/snapshots/{snapshot_id}/restore",
    summary="流式下载该快照恢复出的原始盘镜像",
    responses={200: {"content": {"application/octet-stream": {}}}},
)
def restore_snapshot(snapshot_id: str, service: BackupService = Depends(get_service)):
    iterator, disk_size, block_size, _pin = service.restore_stream(snapshot_id)
    headers = {
        "Content-Length": str(disk_size),
        "X-Block-Size": str(block_size),
        "X-Snapshot-Id": snapshot_id,
    }
    return StreamingResponse(
        iterator,
        media_type="application/octet-stream",
        headers=headers,
    )


@app.post("/gc", response_model=GcResult, summary="手动触发旧块回收（标记-清除）")
def run_gc(service: BackupService = Depends(get_service)):
    return service.run_gc()


@app.get("/health", response_model=HealthOut, summary="健康检查")
def health(service: BackupService = Depends(get_service)):
    return service.health()
