# 单实例分块备份服务

接收单盘按 **4 KiB 分块**的全量备份与后续**线性增量**，支持撤下中间快照（增量合并）、
流式恢复、旧块回收与进程崩溃恢复。FastAPI 实现，Docker Compose 启动，Swagger UI 操作。

## 1. 数据模型与分块协议

* 每份备份是快照链上的一个节点：第一份为全量（无父快照），之后只能基于当前链尾追加增量。
* 上传体是一串帧：`index(uint32 大端) + length(uint32 大端) + length 字节数据`。
* 帧里出现的块下标 = 本快照**显式覆盖**该下标；**未出现 = 沿用父快照**，恢复时沿父链解析。
* 显式上传的全零块与普通数据块无异，照常覆盖父映射（与「缺块沿用」严格区分）。
* 全量备份必须包含全部下标；末块允许不足 4 KiB，恢复镜像按 `disk_size` 精确截断。
* 块以内容 SHA-256 寻址、不可变；相同内容自动去重。

## 2. 合并撤下中间快照

`POST /snapshots/{id}/merge` 撤下指定快照 S，让其直接子增量 C 改挂到 S 的父快照 P：

* 新 C 的块映射 = `S 的显式块` 为底，再用 `C 的显式块` 覆盖，**只处理这两份增量涉及的下标**；
* 其它快照的元数据与块一个都不重写；
* 提交顺序：先原子写新 C 的 JSON，再原子替换 `manifest.json`（rename 原子切换），
  最后删除 S 的元数据并做 GC；
* 合并的准备工作（读链、生成新映射）不持链锁，**期间链尾仍可追加**；
* 链首（全量）和链尾（无子增量）不可撤下；清单写失败时内存与磁盘都保留旧链。

## 3. 恢复与在途保护

* `GET /snapshots/{id}/restore` 流式返回恢复镜像，`Content-Length` 等于原盘字节数。
* 恢复开始即在 `pins/` 目录创建 pin 文件，钉住该快照引用的全部块（含合并中被撤快照
  的元数据），流结束（含客户端中断）的 `finally` 中释放。
* 已开始的恢复即使期间发生合并，也能沿快照 JSON 自带的父指针把**原内容**读完。
* GC 为标记-清除：只回收「无任何保留快照引用 **且** 无在途恢复 pin」的块；
  被 pin 的已撤快照元数据保留到流结束，之后由 GC 清除。

## 4. 崩溃恢复

所有提交物（块、快照 JSON、清单）都走「写 tmp → fsync → rename → fsync 目录」，
进程中断后只会看到完整的旧文件或新文件：

* 启动清空 `tmp/`（中断写入的残留没有任何提交物引用）；
* 以 `manifest.json` 为唯一权威：删除不在清单上的孤儿快照 JSON，
  按清单校正各快照的 `parent_id`，清空遗留 pin，再做一次 GC；
* 因此重启后只能看到**完整的旧链或完整的新链**。

## 5. HTTP 接口（Swagger：`/docs`）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/snapshots` | multipart 表单上传全量/增量（字段：`disk_size`、`block_size`、可选 `parent_id`、`chunks`） |
| GET | `/snapshots` | 列出链上全部快照 |
| POST | `/snapshots/{id}/merge` | 合并并撤下指定中间快照 |
| GET | `/snapshots/{id}/restore` | 流式下载恢复镜像 |
| POST | `/gc` | 手动触发标记-清除 GC |
| GET | `/health` | 健康检查 |

## 6. 运行

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

本地直接运行（无 Docker 时）：

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
python scripts/demo.py            # 端到端手动演示
python -m pytest tests/ -q        # 14 个端到端 + 3 个单元测试
```

> 必须单实例单 worker：进程内锁与 pin 语义均依赖单进程。

## 7. 目录结构

```
app/
  config.py     环境变量配置
  storage.py    内容寻址块仓库、原子写、tmp 清理
  models.py     Pydantic 模型
  service.py    快照链、上传解析、合并、恢复、GC、启动对账
  main.py       FastAPI 路由
scripts/demo.py 手动端到端演示
tests/          子进程级端到端测试 + 服务层故障注入单测
data/           运行期数据（挂载为 Docker 卷，不入库）
```

## 8. 已知限制与未完成事项

* **单实例单 worker 约束**：链锁、上传锁、pin 均为进程内机制，不支持多副本/多 worker
  水平扩展（需求即单实例）。要横向扩展需引入外部锁与 pin 存储（如 Redis/数据库）。
* **上传为整请求缓冲**：当前路由把 multipart 的 `chunks` 一次性读入内存再解析，
  适合 GB 级以内的单盘备份；超大盘需改为流式帧解析（spool 到临时文件），尚未实现。
* **无鉴权/TLS**：服务默认监听明文 HTTP，定位为内网运维工具；对外暴露需前置网关
  加认证与 HTTPS。
* **块大小全局固定**：由 `BLOCK_SIZE` 环境变量决定（默认 4096），同一数据目录内
  所有盘必须一致，不支持混部不同块大小。
* **测试以子进程代替容器**：本环境无 Docker daemon，Dockerfile/Compose 已做 YAML
  与构建逻辑校验，但未实际 `docker compose up`；测试用独立 uvicorn 子进程覆盖了
  与容器等价的启动、重启与崩溃恢复路径。
* **块下标为 uint32**：单盘上限约 2^32 × 4 KiB（约 16 TiB），超出需扩展帧头。

