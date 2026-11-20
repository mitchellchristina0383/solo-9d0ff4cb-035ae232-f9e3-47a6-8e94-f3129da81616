"""商品调价流水的月分区补建。

补建语义（对应 ``POST /partitions/{month}/backfill``）：

* **幂等**：同月重复申请只建一个分区。同一月份的并发补建由事务级
  advisory lock 串行化；后到者等前一个提交后再在锁内复查，发现分区已
  存在就直接返回 ``created=false``，不会再建第二个。
* **原子**：把默认分区里的当月行取出暂存、建月分区、再把暂存行原样写回，
  全部在同一个事务里完成；提交前对外不可见。成功后当月旧流水全部位于
  月分区，其他月份的旧行仍留在默认分区。
* **不丢不重**：补建期间对父表取 ACCESS EXCLUSIVE 锁，直接写父表的请求
  会等锁；PostgreSQL 在锁释放后按新分区重新路由，因此已提交流水既不会
  丢，也不会同时出现在默认分区和新分区。
* **可回滚**：等锁超时、迁移失败或任何错误都会回滚。回滚后旧流水仍在
  默认分区、月分区不存在，分区路由保持不变。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg
from psycopg import sql

logger = logging.getLogger("partitions")

PARENT = "price_changes"
DEFAULT_PARTITION = "price_changes_default"

# 补建事务在父表上等锁的默认预算，以及迁移本身的语句上限。
DEFAULT_LOCK_TIMEOUT = "2s"
DEFAULT_STATEMENT_TIMEOUT = "60s"


class MonthParseError(ValueError):
    """月份参数不是合法的 YYYY-MM。"""


class MonthAlreadyPartitioned(Exception):
    """该月存在另一个名字、边界重叠的月分区。"""


class BackfillLockTimeout(Exception):
    """等待父表锁超时；数据库未做任何改动。"""


class BackfillFailed(Exception):
    """补建事务执行失败；已回滚，旧流水与分区路由保持原状。"""


# 同一月份的并发补建用事务级 advisory lock 串行化（锁随事务结束自动释放，
# 连接异常也不会残留）。不同月份互不影响。
_ADVISORY_NAMESPACE = 0x50524348  # "PRCH"


def _advisory_key(month: "Month") -> str:
    return f"{PARENT}:{month.label}"


@dataclass(frozen=True)
class Month:
    """一个 UTC 自然月，区间为 [start, end)。"""

    year: int
    month: int

    @classmethod
    def parse(cls, text: str) -> "Month":
        m = re.fullmatch(r"(\d{4})-(\d{1,2})", (text or "").strip())
        if not m:
            raise MonthParseError(f"invalid month {text!r}; expected YYYY-MM")
        year, month = int(m.group(1)), int(m.group(2))
        if not 1 <= month <= 12:
            raise MonthParseError(
                f"invalid month {text!r}; month must be 01..12")
        if not 1 <= year <= 9999:
            raise MonthParseError(f"invalid month {text!r}; year out of range")
        return cls(year, month)

    @property
    def start(self) -> datetime:
        return datetime(self.year, self.month, 1, tzinfo=timezone.utc)

    @property
    def end(self) -> datetime:
        if self.month == 12:
            return datetime(self.year + 1, 1, 1, tzinfo=timezone.utc)
        return datetime(self.year, self.month + 1, 1, tzinfo=timezone.utc)

    @property
    def key(self) -> str:
        return f"{self.year:04d}{self.month:02d}"

    @property
    def label(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def partition_name(self) -> str:
        return f"{PARENT}_{self.key}"

    def iso_range(self) -> tuple[str, str]:
        return _literal(self.start), _literal(self.end)


def _literal(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


# ------------------------------------------------------------------ 元数据查询

_CHILDREN_SQL = """
SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bound
FROM pg_inherits AS i
JOIN pg_class   AS c ON c.oid = i.inhrelid
WHERE i.inhparent = %s::regclass
ORDER BY c.relname
"""

_CHILD_NAMES_SQL = """
SELECT c.relname
FROM pg_inherits AS i
JOIN pg_class   AS c ON c.oid = i.inhrelid
WHERE i.inhparent = %s::regclass
ORDER BY c.relname
"""

_BOUND_RE = re.compile(r"FROM \('([^']+)'\) TO \('([^']+)'\)")


def _parse_bounds(bound: str) -> tuple[datetime, datetime] | None:
    m = _BOUND_RE.search(bound or "")
    if not m:
        return None
    lo = datetime.fromisoformat(m.group(1))
    hi = datetime.fromisoformat(m.group(2))
    if lo.tzinfo is None:
        lo = lo.replace(tzinfo=timezone.utc)
    if hi.tzinfo is None:
        hi = hi.replace(tzinfo=timezone.utc)
    return lo, hi


async def _child_rows(conn) -> list[tuple[str, str]]:
    """子分区名与边界表达式。

    注意：``pg_get_expr(relpartbound, ...)`` 会取子表的锁，父表被 ACCESS
    EXCLUSIVE 锁住时会阻塞（且不遵守 lock_timeout）。因此它只应在已经
    持有父表锁的事务内调用；事务外的快速检查只用 :func:`_child_names`。
    """
    async with conn.cursor() as cur:
        await cur.execute(_CHILDREN_SQL, (PARENT,))
        return list(await cur.fetchall())


async def _child_names(conn) -> set[str]:
    """只读名字、不触碰分区边界表达式，因此不会取子表锁。"""
    async with conn.cursor() as cur:
        await cur.execute(_CHILD_NAMES_SQL, (PARENT,))
        return {row[0] for row in await cur.fetchall()}


async def list_partitions(conn: psycopg.AsyncConnection) -> list[dict]:
    """列出父表下的全部分区（含默认分区）。"""
    rows = await _child_rows(conn)
    out: list[dict] = []
    for name, bound in rows:
        bounds = _parse_bounds(bound)
        out.append({
            "name": name,
            "bound": bound,
            "is_default": bound.strip().upper() == "DEFAULT",
            "start": bounds[0].isoformat() if bounds else None,
            "end": bounds[1].isoformat() if bounds else None,
        })
    return out


async def find_partition(
    conn: psycopg.AsyncConnection, month: Month
) -> dict | None:
    """返回该月对应的分区；兼容被手工建成其他名字的情况。

    会读取分区边界表达式（取锁），只应在持有父表锁的事务内使用。
    """
    for name, bound in await _child_rows(conn):
        bounds = _parse_bounds(bound)
        if not bounds:
            continue
        if name == month.partition_name or (
                bounds[0] == month.start and bounds[1] == month.end):
            return {"name": name, "start": bounds[0], "end": bounds[1]}
    return None


async def find_overlapping_partition(
    conn: psycopg.AsyncConnection, month: Month
) -> dict | None:
    """返回任何与 month 区间重叠的月分区（用于拒绝非法/手改的分区）。"""
    for name, bound in await _child_rows(conn):
        bounds = _parse_bounds(bound)
        if not bounds:
            continue
        if bounds[0] < month.end and month.start < bounds[1]:
            return {"name": name, "start": bounds[0], "end": bounds[1]}
    return None


async def count_default_rows(
    conn: psycopg.AsyncConnection, month: Month
) -> int:
    lo, hi = month.iso_range()
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL(
                "SELECT count(*) FROM {tbl} "
                "WHERE changed_at >= %s::timestamptz "
                "  AND changed_at <  %s::timestamptz"
            ).format(tbl=sql.Identifier(DEFAULT_PARTITION)),
            (lo, hi))
        return (await cur.fetchone())[0]


async def count_partition_rows(
    conn: psycopg.AsyncConnection, partition: str, month: Month
) -> int:
    lo, hi = month.iso_range()
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL(
                "SELECT count(*) FROM {tbl} "
                "WHERE changed_at >= %s::timestamptz "
                "  AND changed_at <  %s::timestamptz"
            ).format(tbl=sql.Identifier(partition)),
            (lo, hi))
        return (await cur.fetchone())[0]


# ------------------------------------------------------------------ 补建

_CREATE_PARTITION_SQL = (
    "CREATE TABLE {name} PARTITION OF {parent} "
    "FOR VALUES FROM ({lo}::timestamptz) TO ({hi}::timestamptz)"
)

# 1) 先把当月行从默认分区取出并暂存；默认分区必须清空才能挂载月分区。
_STAGE_ROWS_SQL = (
    "CREATE TEMP TABLE _price_changes_stage ON COMMIT DROP AS "
    "WITH moved AS ("
    "  DELETE FROM {default} "
    "  WHERE changed_at >= {lo}::timestamptz "
    "    AND changed_at <  {hi}::timestamptz "
    "  RETURNING id, item_id, changed_at, new_price, created_at"
    ") SELECT * FROM moved"
)

# 2) 建好月分区后把暂存行原样写入（保留原 id）。
_INSERT_STAGED_SQL = (
    "INSERT INTO {part} (id, item_id, changed_at, new_price, created_at) "
    "SELECT id, item_id, changed_at, new_price, created_at "
    "FROM _price_changes_stage"
)


async def backfill_month(
    conn: psycopg.AsyncConnection,
    month: Month,
    *,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    statement_timeout: str = DEFAULT_STATEMENT_TIMEOUT,
) -> dict:
    """原子补建 month 的分区并迁移当月旧行。

    返回 ``created``、``partition``、``migrated_rows`` 等统计。重复申请
    返回 ``created=False``。等锁超时抛 :class:`BackfillLockTimeout`，
    其他失败抛 :class:`BackfillFailed`；两种情况数据库都保持原样。
    """
    part = month.partition_name
    moved = 0

    # 快速无锁检查只看分区名：pg_get_expr(relpartbound) 会取子表锁、在父表
    # 被锁住时无限等待且不遵守 lock_timeout，绝不能放在加锁事务之外。
    if part in await _child_names(conn):
        return await _result(conn, month, created=False, migrated=0)

    try:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT set_config('lock_timeout', %s, true)",
                    (lock_timeout,))
                await cur.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (statement_timeout,))
                # 同月补建串行化：后到者在此排队，等前一个提交后进入锁内
                # 复查并返回 created=false，不会重复建分区。
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, %s))",
                    (_advisory_key(month), _ADVISORY_NAMESPACE))
                # 锁父表：与“路由判定 + 插入”互斥。写请求可能在此等锁。
                await cur.execute(
                    sql.SQL("LOCK TABLE ONLY {parent} IN ACCESS EXCLUSIVE MODE")
                    .format(parent=sql.Identifier(PARENT)))
                # 直接写默认分区的请求只锁子表，因此再显式锁一次；锁序
                # 为“父表 -> 默认分区”，与 INSERT 的加锁顺序一致，无死锁。
                await cur.execute(
                    sql.SQL("LOCK TABLE {tbl} IN ACCESS EXCLUSIVE MODE")
                    .format(tbl=sql.Identifier(DEFAULT_PARTITION)))

                # 锁内复查：并发补建者可能已经建好 -> 幂等返回。
                existing = await find_partition(conn, month)
                if existing:
                    if existing["name"] != part:
                        raise MonthAlreadyPartitioned(existing["name"])
                    return await _result(conn, month, created=False, migrated=0)
                overlap = await find_overlapping_partition(conn, month)
                if overlap:
                    raise MonthAlreadyPartitioned(overlap["name"])

                lo, hi = month.iso_range()
                # 顺序很重要：默认分区里残留当月行时 CREATE ... PARTITION OF
                # 会被拒绝（"updated partition constraint ... would be
                # violated"），所以先搬走、再建分区、最后把暂存行写进新分区。
                await cur.execute(
                    sql.SQL(_STAGE_ROWS_SQL).format(
                        default=sql.Identifier(DEFAULT_PARTITION),
                        lo=sql.Literal(lo), hi=sql.Literal(hi)))
                moved = cur.rowcount
                await cur.execute(
                    sql.SQL(_CREATE_PARTITION_SQL).format(
                        name=sql.Identifier(part),
                        parent=sql.Identifier(PARENT),
                        lo=sql.Literal(lo), hi=sql.Literal(hi)))
                await cur.execute(
                    sql.SQL(_INSERT_STAGED_SQL).format(
                        part=sql.Identifier(part)))
                if cur.rowcount != moved:
                    raise BackfillFailed(
                        f"staged {moved} rows but inserted {cur.rowcount}; "
                        f"rolling back")

                # 提交前自检：当月行不得再留在默认分区。
                remaining = await count_default_rows(conn, month)
                if remaining != 0:
                    raise BackfillFailed(
                        f"{remaining} rows for {month.label} still in "
                        f"{DEFAULT_PARTITION}; rolling back")
                logger.info("backfill %s committed", month.label)
    except psycopg.errors.LockNotAvailable as exc:
        # 等锁超时：事务已回滚。若别人刚好补建成功，视为幂等成功。
        if part in await _child_names(conn):
            return await _result(conn, month, created=False, migrated=0)
        raise BackfillLockTimeout(
            f"could not lock {PARENT} within {lock_timeout}; "
            f"no changes were made") from exc
    except psycopg.errors.DuplicateTable as exc:
        if part in await _child_names(conn):
            return await _result(conn, month, created=False, migrated=0)
        raise BackfillFailed(str(exc).strip()) from exc
    except (MonthAlreadyPartitioned, BackfillFailed):
        raise
    except Exception as exc:  # noqa: BLE001 - 统一为可重试的失败
        logger.exception("backfill %s failed", month.label)
        raise BackfillFailed(str(exc).strip()) from exc

    return await _result(conn, month, created=True, migrated=moved)


async def _result(
    conn: psycopg.AsyncConnection,
    month: Month,
    *,
    created: bool,
    migrated: int,
) -> dict:
    found = await find_partition(conn, month)
    name = found["name"] if found else month.partition_name
    total = (await count_partition_rows(conn, name, month)) if found else 0
    remaining = await count_default_rows(conn, month)
    return {
        "created": created,
        "already_existed": not created,
        "month": month.label,
        "partition": name,
        "migrated_rows": migrated,
        "partition_rows_in_month": total,
        "remaining_default_rows": remaining,
    }
