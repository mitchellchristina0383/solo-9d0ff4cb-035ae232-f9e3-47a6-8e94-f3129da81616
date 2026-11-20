"""报价流水 API：按月分区保管商品调价流水。

除原有的只读查询 / 连接池演示外，本服务还提供：

- 沿用现有商品编号（``items.id``）录入调价时间与新价格；
- 按 UTC 月查询流水，并返回每行实际所在分区；
- 为指定月份补建月分区：成功后当月旧流水全部迁入，其他月份留在原处；
- 同月重复申请幂等；补建与写入交错不丢不重；等锁超时/失败则原样保留。

查询端点走只读连接池（会话级 ``default_transaction_read_only=on``）；
写入与分区维护走独立的写池。
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal

import psycopg
from fastapi import FastAPI, Path as PathParam, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator

import partitions
from partitions import (
    BackfillFailed,
    BackfillLockTimeout,
    Month,
    MonthAlreadyPartitioned,
    MonthParseError,
)
from pool import AcquireTimeoutError, AsyncPool, PoolClosedError, PoolConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("api")


# ---------------------------------------------------------------- 异常类型

class ClientGone(Exception):
    """客户端在排队或查询期间断开连接。"""


class QueryTimeout(Exception):
    """查询未在请求给定的超时时间内完成。"""


class SqlFailed(Exception):
    """SQL 执行报错。status_code 区分客户端 SQL 错误(400)与连接故障(502)。"""

    def __init__(self, detail: str, status_code: int = 400):
        super().__init__(detail)
        self.status_code = status_code


# ---------------------------------------------------------------- 请求模型

class QueryOpts(BaseModel):
    acquire_timeout: float | None = Field(
        10.0, description="等待连接的最长时间（秒），null 表示不限")
    query_timeout: float | None = Field(
        30.0, description="查询执行超时（秒），null 表示不限")


class NormalQuery(QueryOpts):
    limit: int = Field(5, ge=1, le=100, description="返回行数")


class SlowQuery(QueryOpts):
    delay_seconds: float = Field(
        5.0, ge=0, le=120, description="pg_sleep 延时秒数")


class ErrorQuery(QueryOpts):
    mode: Literal["autocommit", "transaction"] = Field(
        "autocommit",
        description="autocommit=单语句报错；transaction=BEGIN 后报错，留下失败事务")


class Burst(BaseModel):
    n: int = Field(8, ge=1, le=64, description="并发慢查询数量")
    delay_seconds: float = Field(3.0, ge=0, le=120)
    acquire_timeout: float | None = Field(30.0, description="每个请求的等连接超时")


class PriceChangeIn(BaseModel):
    item_id: int = Field(..., ge=1, description="沿用现有商品编号 items.id")
    changed_at: datetime = Field(..., description="调价时间（带时区；按 UTC 归月）")
    new_price: Decimal = Field(..., gt=0, le=Decimal("99999999.99"),
                               description="新价格，最多两位小数")

    @field_validator("changed_at")
    @classmethod
    def _require_tz(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(
                "changed_at must include a timezone offset, e.g. "
                "2026-11-20T10:00:00Z")
        return value.astimezone(timezone.utc)

    @field_validator("new_price")
    @classmethod
    def _two_dp(cls, value: Decimal) -> Decimal:
        if value != value.quantize(Decimal("0.01")):
            raise ValueError("new_price accepts at most 2 decimal places")
        return value


class PriceChangeList(QueryOpts):
    limit: int = Field(100, ge=1, le=1000, description="最多返回行数")


# ---------------------------------------------------------------- 应用装配

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "db" / "schema.sql"


def _dsn() -> dict:
    return dict(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        dbname=os.environ.get("PGDATABASE", "demo"),
        user=os.environ.get("PGUSER", "app"),
        password=os.environ.get("PGPASSWORD", "app"),
    )


async def _ensure_schema() -> None:
    """确保调价流水表存在（容器首次启动时 init.sql 已建；这里兜底）。

    用独立连接执行幂等 DDL；多副本同时启动时可能撞上重复建表，重试几次
    即可——最终每个对象都只会存在一份。
    """
    sql_text = SCHEMA_FILE.read_text(encoding="utf-8")
    last_exc: Exception | None = None
    for attempt in range(5):
        conn = await psycopg.AsyncConnection.connect(
            **_dsn(), autocommit=True,
            application_name="schema-init",
            options="-c TimeZone=UTC -c DateStyle=ISO",
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(sql_text)
            return
        except (psycopg.errors.DuplicateTable,
                psycopg.errors.DuplicateObject,
                psycopg.errors.UniqueViolation) as exc:
            # 另一个副本正在建同一对象：稍等后重跑幂等脚本
            last_exc = exc
            logger.warning("schema init race (attempt %d): %s",
                           attempt + 1, type(exc).__name__)
            await asyncio.sleep(0.5 * (attempt + 1))
        finally:
            await conn.close()
    assert last_exc is not None
    raise last_exc


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _ensure_schema()
    read_pool = AsyncPool(PoolConfig(
        size=int(os.environ.get("POOL_SIZE", "4")),
        dsn=_dsn(),
        read_only=True,
        application_name="ro-api",
        # UTC + ISO：分区的月份边界与查询都按 UTC 解释
        server_options="-c TimeZone=UTC -c DateStyle=ISO",
    ))
    write_pool = AsyncPool(PoolConfig(
        size=int(os.environ.get("WRITE_POOL_SIZE", "2")),
        dsn=_dsn(),
        read_only=False,
        application_name="price-writer",
        server_options="-c TimeZone=UTC -c DateStyle=ISO",
    ))
    await read_pool.start()
    await write_pool.start()
    app.state.pool = read_pool        # 原有查询端点与 /pool/stats 使用的池
    app.state.write_pool = write_pool
    logger.info("read pool ready: %s", await read_pool.stats())
    logger.info("write pool ready: %s", await write_pool.stats())
    yield
    await read_pool.close()
    await write_pool.close()


app = FastAPI(
    title="报价流水 API（商品调价 + 按月分区）",
    description=(
        "沿用 items 商品编号录入调价流水，按 UTC 月查询并返回所在分区；"
        "可为指定月份补建分区（幂等、原子迁移、失败回滚）。"
        "另保留原有的只读查询与连接池演示接口。"
    ),
    lifespan=lifespan,
)


@app.exception_handler(AcquireTimeoutError)
async def _on_acquire_timeout(req: Request, exc: AcquireTimeoutError):
    return JSONResponse(
        {"error": "acquire_timeout", "detail": str(exc)}, status_code=503)


@app.exception_handler(QueryTimeout)
async def _on_query_timeout(req: Request, exc: QueryTimeout):
    return JSONResponse(
        {"error": "query_timeout", "detail": str(exc)}, status_code=504)


@app.exception_handler(SqlFailed)
async def _on_sql_failed(req: Request, exc: SqlFailed):
    return JSONResponse(
        {"error": "sql_error", "detail": str(exc)}, status_code=exc.status_code)


@app.exception_handler(ClientGone)
async def _on_client_gone(req: Request, exc: ClientGone):
    # 客户端已经断开，响应大概率发不出去，状态码仅作记录用途。
    return JSONResponse({"error": "client_disconnected"}, status_code=499)


# ---------------------------------------------------------------- 核心执行逻辑

async def _watch_disconnect(request: Request) -> None:
    while not await request.is_disconnected():
        await asyncio.sleep(0.1)


async def _run_query(
    request: Request,
    opts: QueryOpts,
    fn: Callable[[psycopg.AsyncConnection], Awaitable[dict]],
    *,
    pool_attr: str = "pool",
) -> Any:
    """借一条池化连接并执行 fn(conn)，带独立的两段超时。

    - 等连接与执行查询的超时互不影响；
    - 排队时客户端断开：队列名额立即释放；
    - 查询超时 / 客户端断开 / SQL 报错：连接按“脏”归还，由连接池
      清理验证后再复用或更换。

    ``pool_attr`` 选择只读池（``pool``）或写池（``write_pool``）。
    """
    pool: AsyncPool = getattr(request.app.state, pool_attr)
    disconnect = asyncio.create_task(_watch_disconnect(request))
    conn = None
    query: asyncio.Task | None = None
    started = time.monotonic()
    try:
        # ---- 阶段 1：借连接（受 acquire_timeout 约束）
        acquire = asyncio.create_task(pool.acquire(opts.acquire_timeout))
        done, _ = await asyncio.wait(
            {acquire, disconnect}, return_when=asyncio.FIRST_COMPLETED)
        if disconnect in done:
            if not acquire.done():
                acquire.cancel()
                with suppress(asyncio.CancelledError, AcquireTimeoutError,
                              PoolClosedError):
                    await acquire
                pool.counters["queue_cancelled"] += 1
            elif acquire.exception() is None:
                # 竞态：刚拿到连接但客户端已离开，干净归还
                await pool.release(acquire.result(), dirty=False)
                pool.counters["queue_cancelled"] += 1
            raise ClientGone()
        conn = acquire.result()  # 可能抛 AcquireTimeoutError -> 503

        # ---- 阶段 2：执行查询（受 query_timeout 约束）
        query = asyncio.create_task(fn(conn))
        done, _ = await asyncio.wait(
            {query, disconnect}, timeout=opts.query_timeout,
            return_when=asyncio.FIRST_COMPLETED)
        if disconnect in done:
            pool.counters["client_disconnects"] += 1
            await pool.release(conn, dirty=True, pending=query)
            conn = None
            raise ClientGone()
        if query not in done:
            pool.counters["query_timeouts"] += 1
            await pool.release(conn, dirty=True, pending=query)
            conn = None
            raise QueryTimeout(
                f"query did not finish within {opts.query_timeout}s")
        try:
            result = query.result()
        except psycopg.Error as exc:
            pool.counters["sql_errors"] += 1
            await pool.release(conn, dirty=True)
            conn = None
            status = 502 if isinstance(exc, psycopg.OperationalError) else 400
            raise SqlFailed(f"{type(exc).__name__}: {exc}", status) from exc
        await pool.release(conn, dirty=False)
        conn = None
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        return result
    finally:
        disconnect.cancel()
        with suppress(asyncio.CancelledError):
            await disconnect
        if conn is not None:  # 意外退出（如服务关停）时按脏连接归还
            await pool.release(conn, dirty=True, pending=query)


# ---------------------------------------------------------------- 接口

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/docs")


@app.post("/query/normal", summary="普通查询")
async def query_normal(request: Request, opts: NormalQuery | None = None):
    opts = opts or NormalQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, name, price FROM items ORDER BY id LIMIT %s",
                (opts.limit,))
            rows = await cur.fetchall()
        return {"rows": [{"id": r[0], "name": r[1], "price": float(r[2])}
                         for r in rows]}

    return await _run_query(request, opts, run)


@app.post("/query/slow", summary="延时查询（pg_sleep）")
async def query_slow(request: Request, opts: SlowQuery | None = None):
    opts = opts or SlowQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_sleep(%s)", (opts.delay_seconds,))
            await cur.fetchall()
        return {"slept_seconds": opts.delay_seconds}

    return await _run_query(request, opts, run)


@app.post("/query/error", summary="报错查询")
async def query_error(request: Request, opts: ErrorQuery | None = None):
    opts = opts or ErrorQuery()

    async def run(conn):
        async with conn.cursor() as cur:
            if opts.mode == "transaction":
                # 故意留下一个失败的事务，考验连接池的清理
                await cur.execute("BEGIN")
            await cur.execute("SELECT 1/0")
        return {}  # 不会到达

    return await _run_query(request, opts, run)


@app.get("/pool/stats", summary="连接池指标（在用数、等待数等）")
async def pool_stats(request: Request):
    return await request.app.state.pool.stats()


@app.post("/demo/burst", summary="演示：并发打满连接池，观察排队")
async def demo_burst(request: Request, opts: Burst | None = None):
    """后台并发发起 n 个慢查询，配合 GET /pool/stats 观察 waiting/in_use。"""
    opts = opts or Burst()
    pool: AsyncPool = request.app.state.pool
    for _ in range(opts.n):
        pool.spawn(_burst_one(pool, opts))
    return {"launched": opts.n,
            "hint": "马上反复查询 GET /pool/stats 观察 in_use / waiting"}


async def _burst_one(pool: AsyncPool, opts: Burst) -> None:
    try:
        conn = await pool.acquire(opts.acquire_timeout)
    except Exception:
        return
    try:
        async with conn.cursor() as cur:
            await cur.execute("SELECT pg_sleep(%s)", (opts.delay_seconds,))
            await cur.fetchall()
    except Exception:
        await pool.release(conn, dirty=True)
        return
    await pool.release(conn, dirty=False)


# ---------------------------------------------------------------- 调价流水

@app.exception_handler(MonthParseError)
async def _on_bad_month(req: Request, exc: MonthParseError):
    return JSONResponse(
        {"error": "invalid_month", "detail": str(exc)}, status_code=400)


@app.exception_handler(MonthAlreadyPartitioned)
async def _on_month_conflict(req: Request, exc: MonthAlreadyPartitioned):
    return JSONResponse(
        {"error": "partition_conflict",
         "detail": f"month already covered by partition {exc}"},
        status_code=409)


@app.exception_handler(BackfillLockTimeout)
async def _on_backfill_lock_timeout(req: Request, exc: BackfillLockTimeout):
    return JSONResponse(
        {"error": "lock_timeout", "detail": str(exc)}, status_code=503)


@app.exception_handler(BackfillFailed)
async def _on_backfill_failed(req: Request, exc: BackfillFailed):
    return JSONResponse(
        {"error": "backfill_failed", "detail": str(exc)}, status_code=500)


@app.post("/price-changes", summary="录入一条调价流水")
async def add_price_change(request: Request, body: PriceChangeIn):
    """沿用现有商品编号写入调价时间与新价格。

    月分区未建时写入默认分区；已建月分区时直接写入该月分区。响应里的
    ``partition`` 是这一行实际落入的分区。
    """

    async def run(conn):
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM items WHERE id = %s", (body.item_id,))
            if await cur.fetchone() is None:
                raise SqlFailed(f"item_id {body.item_id} not found", 404)
            await cur.execute(
                "INSERT INTO price_changes (item_id, changed_at, new_price) "
                "VALUES (%s, %s, %s) "
                "RETURNING id, item_id, changed_at, new_price, "
                "          tableoid::regclass::text AS partition",
                (body.item_id, body.changed_at, body.new_price))
            row = await cur.fetchone()
        return {
            "id": row[0],
            "item_id": row[1],
            "changed_at": row[2].astimezone(timezone.utc).isoformat(),
            "new_price": str(row[3]),
            "partition": row[4],
        }

    return await _run_query(request, PriceChangeList(), run,
                            pool_attr="write_pool")


@app.get("/price-changes", summary="按 UTC 月查询流水及其所在分区")
async def list_price_changes(
    request: Request,
    month: str = Query(..., description="UTC 月份 YYYY-MM", examples=["2026-11"]),
    limit: int = Query(100, ge=1, le=1000, description="最多返回行数"),
    acquire_timeout: float | None = Query(10.0),
    query_timeout: float | None = Query(30.0),
):
    m = Month.parse(month)
    opts = PriceChangeList(
        limit=limit, acquire_timeout=acquire_timeout,
        query_timeout=query_timeout)

    async def run(conn):
        lo, hi = m.iso_range()
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, item_id, changed_at, new_price, created_at, "
                "       tableoid::regclass::text AS partition "
                "FROM price_changes "
                "WHERE changed_at >= %s::timestamptz "
                "  AND changed_at <  %s::timestamptz "
                "ORDER BY changed_at, id LIMIT %s",
                (lo, hi, limit))
            rows = await cur.fetchall()
        return {
            "month": m.label,
            "row_count": len(rows),
            "rows": [{
                "id": r[0],
                "item_id": r[1],
                "changed_at": r[2].astimezone(timezone.utc).isoformat(),
                "new_price": str(r[3]),
                "created_at": r[4].astimezone(timezone.utc).isoformat(),
                "partition": r[5],
            } for r in rows],
        }

    return await _run_query(request, opts, run, pool_attr="write_pool")


@app.get("/partitions", summary="列出调价流水的全部分区")
async def show_partitions(
    request: Request,
    acquire_timeout: float | None = Query(10.0),
    query_timeout: float | None = Query(30.0),
):
    opts = QueryOpts(acquire_timeout=acquire_timeout,
                     query_timeout=query_timeout)

    async def run(conn):
        parts = await partitions.list_partitions(conn)
        return {"partitions": [{
            "name": p["name"],
            "is_default": p["is_default"],
            "start": p["start"],
            "end": p["end"],
        } for p in parts]}

    return await _run_query(request, opts, run, pool_attr="write_pool")


@app.post("/partitions/{month}/backfill",
          summary="补建指定 UTC 月份的分区并迁移当月旧流水")
async def backfill_partition(
    request: Request,
    month: str = PathParam(..., description="UTC 月份 YYYY-MM",
                           examples=["2026-11"]),
    lock_timeout: str = Query(
        partitions.DEFAULT_LOCK_TIMEOUT,
        description="等待父表锁的上限（PostgreSQL 时长，如 2s、500ms）"),
    acquire_timeout: float | None = Query(10.0),
    query_timeout: float | None = Query(60.0),
):
    """幂等：同月重复申请只建一个分区。

    - 成功：当月旧流水全部迁入月分区，其他月份留在默认分区；
    - 已有该月分区：返回 ``created=false``；
    - 等锁超时 / 迁移失败：返回 503 / 500，旧流水与分区路由保持原样。
    """
    m = Month.parse(month)
    opts = QueryOpts(acquire_timeout=acquire_timeout,
                     query_timeout=query_timeout)

    async def run(conn):
        return await partitions.backfill_month(
            conn, m, lock_timeout=lock_timeout)

    return await _run_query(request, opts, run, pool_attr="write_pool")
