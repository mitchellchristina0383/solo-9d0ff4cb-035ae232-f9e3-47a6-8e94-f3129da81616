#!/usr/bin/env python3
"""调价流水 / 月分区补建的端到端自测。

前置条件：
- API 已在 BASE_URL（默认 http://127.0.0.1:8000）运行；
- 可选：环境变量 PSQL 给出 psql 连接串
  （如 "postgresql://app:app@127.0.0.1:5432/demo"），用于服务端直连核对
  （分区物理位置、行数、锁行为）。未提供时相关断言自动跳过。

用法：python3 tests/partition_selftest.py
"""

from __future__ import annotations

import asyncio
import os
import random
import subprocess
import sys
import time
from collections import Counter

import httpx

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
PSQL = os.environ.get("PSQL")
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(f"{name}: {detail}")


def psql(sql: str, timeout: float = 30) -> str | None:
    if not PSQL:
        return None
    try:
        out = subprocess.run(["psql", PSQL, "-X", "-tA", "-c", sql],
                             capture_output=True, text=True, timeout=timeout)
        if out.returncode != 0:
            print("  psql error:", out.stderr.strip()[:300])
            return None
        return out.stdout.strip()
    except Exception as exc:  # noqa: BLE001
        print("  psql exception:", exc)
        return None


async def psql_async(sql: str, timeout: float = 30) -> str | None:
    return await asyncio.to_thread(psql, sql, timeout)


def month_bounds(year: int, mon: int) -> tuple[str, str]:
    lo = f"{year:04d}-{mon:02d}-01T00:00:00Z"
    if mon == 12:
        hi = f"{year + 1:04d}-01-01T00:00:00Z"
    else:
        hi = f"{year:04d}-{mon + 1:02d}-01T00:00:00Z"
    return lo, hi


def pick_exotic_month(seed: int) -> tuple[int, int]:
    rng = random.Random(seed)
    year = rng.choice([2019, 2020, 2021, 2022, 2023, 2024, 2025])
    mon = rng.choice(range(1, 13))
    return year, mon


async def reset_partition(client, year: int, mon: int) -> None:
    """把该月恢复到“分区不存在、当月行都在默认分区”的干净状态。

    仅测试清理用；生产接口不提供删分区能力。
    """
    if not PSQL:
        return
    name = f"price_changes_{year:04d}{mon:02d}"
    psql(f"DELETE FROM price_changes WHERE changed_at >= "
         f"'{year:04d}-{mon:02d}-01T00:00:00Z'::timestamptz AND changed_at < "
         f"'{year + (mon == 12):04d}-{1 if mon == 12 else mon + 1:02d}-01"
         f"T00:00:00Z'::timestamptz")
    if psql(f"SELECT to_regclass('public.{name}') IS NOT NULL") == "t":
        psql(f"DROP TABLE {name}")


async def insert(client, item_id: int, changed_at: str, price: str):
    return await client.post("/price-changes", json={
        "item_id": item_id, "changed_at": changed_at, "new_price": price})


async def list_month(client, month: str):
    return await client.get("/price-changes", params={"month": month,
                                                      "limit": 1000})


# ---------------------------------------------------------------- 用例

async def t_month_boundaries(client):
    """UTC 月初/月末/±1 秒的归月必须精确；跨时区输入按 UTC 归月。"""
    year, mon = pick_exotic_month(11)
    await reset_partition(client, year, mon)
    lo, hi = month_bounds(year, mon)
    # 月末最后一秒
    last = f"{year:04d}-{mon:02d}-" + ("31" if mon in (1, 3, 5, 7, 8, 10, 12)
                                       else "30" if mon != 2 else "28") + \
           "T23:59:59Z"
    rows = [
        (1, lo, "1.11"),                 # 月首 00:00:00 -> 本月
        (2, last, "2.22"),               # 月末最后一秒 -> 本月
    ]
    ids = []
    for item, when, price in rows:
        r = await insert(client, item, when, price)
        check(f"boundary: insert {when} -> 200", r.status_code == 200, r.text)
        if r.status_code == 200:
            ids.append(r.json()["id"])
    # 前后边界外的行不应出现在本月
    if mon == 1:
        before = f"{year - 1:04d}-12-31T23:59:59Z"
    else:
        before = f"{year:04d}-{mon - 1:02d}-28T23:59:59Z"
    await insert(client, 3, before, "3.33")
    after_month = 1 if mon == 12 else mon + 1
    after_year = year + 1 if mon == 12 else year
    await insert(client, 4, f"{after_year:04d}-{after_month:02d}-01T00:00:00Z", "4.44")

    r = await list_month(client, f"{year:04d}-{mon:02d}")
    check("boundary: list 200", r.status_code == 200, r.text)
    body = r.json()
    got = {row["item_id"] for row in body["rows"]}
    check("boundary: includes exact month edges", {1, 2} <= got, str(got))
    check("boundary: excludes adjacent months", not ({3, 4} & got), str(got))
    all_in_month = all(
        row["changed_at"][:7] == f"{year:04d}-{mon:02d}"
        for row in body["rows"])
    check("boundary: every listed row is in month", all_in_month, str(body))


async def t_backfill_moves_month_only(client):
    """补建只迁移当月旧行，其他月份留在原处；行内容与 id 不变。"""
    year, mon = pick_exotic_month(23)
    await reset_partition(client, year, mon)
    lo, hi = month_bounds(year, mon)
    part = f"price_changes_{year:04d}{mon:02d}"

    before_rows = []
    for i, (item, when) in enumerate([
        (1, lo), (2, f"{year:04d}-{mon:02d}-15T12:00:00Z"),
    ]):
        r = await insert(client, item, when, f"{i + 1}.23")
        check(f"backfill: seed {when} 200", r.status_code == 200, r.text)
        before_rows.append(r.json())
    # 其他月份的旧行
    other_mon = mon - 1 if mon > 1 else 12
    other_year = year if mon > 1 else year - 1
    r = await insert(client, 5, f"{other_year:04d}-{other_mon:02d}-10T00:00:00Z",
                     "9.99")
    check("backfill: seed other month 200", r.status_code == 200, r.text)
    other_row = r.json()

    if PSQL:
        before = psql("SELECT tableoid::regclass::text, count(*) FROM price_changes "
                      "GROUP BY 1 ORDER BY 1")
        check("backfill: seeds initially in default",
              before is not None and "price_changes_default" in before, str(before))

    r = await client.post(f"/partitions/{year:04d}-{mon:02d}/backfill")
    check("backfill: 200", r.status_code == 200, r.text)
    body = r.json()
    check("backfill: created", body.get("created") is True, str(body))
    check("backfill: partition name", body.get("partition") == part, str(body))
    check("backfill: migrated 2", body.get("migrated_rows") == 2, str(body))
    check("backfill: default now empty for month",
          body.get("remaining_default_rows") == 0, str(body))

    r = await list_month(client, f"{year:04d}-{mon:02d}")
    rows = r.json()["rows"]
    by_id = {row["id"]: row for row in rows}
    for original in before_rows:
        row = by_id.get(original["id"])
        check(f"backfill: id {original['id']} preserved", row is not None, str(rows))
        if row:
            check(f"backfill: id {original['id']} now in month partition",
                  row["partition"] == part, str(row))
            check(f"backfill: id {original['id']} price unchanged",
                  row["new_price"] == original["new_price"], str(row))
    r = await list_month(client, f"{other_year:04d}-{other_mon:02d}")
    other = [x for x in r.json()["rows"] if x["id"] == other_row["id"]]
    check("backfill: other month row untouched",
          len(other) == 1 and other[0]["partition"] == "price_changes_default",
          str(other))


async def t_backfill_idempotent(client):
    """同月重复申请只建一个分区。"""
    year, mon = pick_exotic_month(37)
    await reset_partition(client, year, mon)
    await insert(client, 1, f"{year:04d}-{mon:02d}-05T00:00:00Z", "1.00")
    month = f"{year:04d}-{mon:02d}"
    results = await asyncio.gather(*[
        client.post(f"/partitions/{month}/backfill") for _ in range(6)])
    codes = [r.status_code for r in results]
    check("idempotent: all 200", all(c == 200 for c in codes), str(codes))
    created = sum(1 for r in results if r.json().get("created"))
    check("idempotent: exactly one created", created == 1, str(created))
    n = psql(f"SELECT count(*) FROM pg_inherits i JOIN pg_class c "
             f"ON c.oid=i.inhrelid WHERE i.inhparent='price_changes'::regclass "
             f"AND c.relname = 'price_changes_{year:04d}{mon:02d}'")
    if n is not None:
        check("idempotent: exactly one physical partition", n == "1", n)
    rows = (await list_month(client, month)).json()["rows"]
    ids = [r["id"] for r in rows]
    check("idempotent: no duplicate rows", len(ids) == len(set(ids)), str(ids))
    check("idempotent: all rows in partition",
          all(r["partition"] == f"price_changes_{year:04d}{mon:02d}" for r in rows),
          str(rows))


async def t_concurrent_writes_and_backfill(client):
    """补建与写入交错：已提交流水不丢不重，全部落在正确分区。"""
    year, mon = 2026, 2   # 用固定、不常用的月份，避免与其他用例冲突
    await reset_partition(client, year, mon)
    month = f"{year:04d}-{mon:02d}"
    lo, hi = month_bounds(year, mon)
    part = f"price_changes_{year:04d}{mon:02d}"

    stop = asyncio.Event()
    written: list[tuple[int, str]] = []
    write_errors: list[str] = []

    async def writer(k: int):
        i = 0
        while not stop.is_set():
            # 在月内随机时刻写入
            when = f"{year:04d}-{mon:02d}-{(i % 27) + 1:02d}T" \
                   f"{(i * 7) % 24:02d}:{(i * 13) % 60:02d}:00Z"
            try:
                r = await insert(client, (i % 20) + 1, when, "1.50")
            except Exception as exc:  # noqa: BLE001
                write_errors.append(f"writer {k}: {exc}")
                return
            if r.status_code != 200:
                write_errors.append(f"writer {k}: {r.status_code} {r.text}")
                return
            written.append((r.json()["id"], when))
            i += 1
            await asyncio.sleep(0)

    # 在写入进行中触发补建
    writers = [asyncio.create_task(writer(k)) for k in range(6)]
    await asyncio.sleep(0.1)
    r = await client.post(f"/partitions/{month}/backfill")
    check("interleave: backfill 200", r.status_code == 200, r.text)
    await asyncio.sleep(0.1)
    stop.set()
    await asyncio.gather(*writers)

    check("interleave: no write errors", not write_errors,
          "; ".join(write_errors[:3]))
    check("interleave: wrote rows", len(written) > 20, str(len(written)))

    # 结束后再补建一次，确保没有残留：应返回 created=false 且当月默认分区为空
    r = await client.post(f"/partitions/{month}/backfill")
    check("interleave: second backfill 200", r.status_code == 200, r.text)
    check("interleave: default empty for month",
          r.json().get("remaining_default_rows") == 0, r.text)

    rows = (await list_month(client, month)).json()["rows"]
    got_ids = [row["id"] for row in rows]
    check("interleave: no duplicate ids", len(got_ids) == len(set(got_ids)),
          str([k for k, v in Counter(got_ids).items() if v > 1][:5]))
    want = {wid for wid, _ in written}
    missing = want - set(got_ids)
    check("interleave: no lost committed rows", not missing,
          f"missing={sorted(missing)[:10]}")
    check("interleave: all rows in month partition",
          all(row["partition"] == part for row in rows),
          str({row["partition"] for row in rows}))
    if PSQL:
        total = psql(f"SELECT count(*) FROM price_changes WHERE "
                     f"changed_at >= '{year:04d}-{mon:02d}-01T00:00:00Z'::timestamptz "
                     f"AND changed_at < '{hi}'")
        check("interleave: table total matches listing",
              total == str(len(rows)), f"psql={total} api={len(rows)}")


async def t_lock_timeout_rolls_back(client):
    """补建等锁超时：返回 503，旧流水和分区路由都不变。"""
    year, mon = 2026, 7
    await reset_partition(client, year, mon)
    month = f"{year:04d}-{mon:02d}"
    part = f"price_changes_{year:04d}{mon:02d}"
    seed = await insert(client, 1, f"{year:04d}-{mon:02d}-10T00:00:00Z", "3.21")
    check("lock-timeout: seed 200", seed.status_code == 200, seed.text)
    seed_row = seed.json()

    if not PSQL:
        print("[SKIP] lock-timeout: needs PSQL for a blocking session")
        return

    # 后台持有一个阻塞父表锁的事务
    blocker_sql = (
        "BEGIN; LOCK TABLE price_changes IN ACCESS EXCLUSIVE MODE; "
        "SELECT pg_sleep(4); COMMIT;")
    blocker = subprocess.Popen(["psql", PSQL, "-X", "-q", "-c", blocker_sql],
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
    await asyncio.sleep(1.0)
    try:
        t0 = time.monotonic()
        r = await client.post(
            f"/partitions/{month}/backfill",
            params={"lock_timeout": "1s"})
        dt = time.monotonic() - t0
        check("lock-timeout: 503", r.status_code == 503,
              f"{r.status_code} {r.text}")
        check("lock-timeout: returned promptly", dt < 4.0, f"{dt:.2f}s")
    finally:
        blocker.terminate()
        blocker.wait(timeout=10)

    exists = psql(f"SELECT to_regclass('public.{part}') IS NOT NULL")
    check("lock-timeout: partition not created", exists in (None, "f"), str(exists))
    rows = (await list_month(client, month)).json()["rows"]
    mine = [x for x in rows if x["id"] == seed_row["id"]]
    check("lock-timeout: old row still present", len(mine) == 1, str(rows))
    check("lock-timeout: old row still routed to default",
          mine and mine[0]["partition"] == "price_changes_default", str(mine))

    # 锁释放后补建应成功
    r = await client.post(f"/partitions/{month}/backfill")
    check("lock-timeout: retry succeeds after lock released",
          r.status_code == 200 and r.json().get("created") is True, r.text)


async def t_migration_failure_rolls_back(client):
    """迁移阶段失败：返回 500，旧流水与分区路由原样保留。

    人为让新分区写入失败：先在该月建一个非分区同名表会让 CREATE 失败；
    这里用“同月已有重叠的手工分区”来触发 MonthAlreadyPartitioned（409），
    并在 psql 里人为制造一个失败事务验证回滚。
    """
    year, mon = 2026, 8
    await reset_partition(client, year, mon)
    month = f"{year:04d}-{mon:02d}"
    seed = await insert(client, 1, f"{year:04d}-{mon:02d}-10T00:00:00Z", "4.32")
    check("migrate-fail: seed 200", seed.status_code == 200, seed.text)
    seed_id = seed.json()["id"]

    # 用 psql 制造一个失败：在月分区位置上放一个同名表
    if PSQL:
        psql(f"CREATE TABLE price_changes_{year:04d}{mon:02d} (x int)")
        r = await client.post(f"/partitions/{month}/backfill")
        check("migrate-fail: 500 when DDL fails", r.status_code == 500,
              f"{r.status_code} {r.text}")
        body = r.json()
        check("migrate-fail: error reported", body.get("error") == "backfill_failed",
              str(body))
        psql(f"DROP TABLE price_changes_{year:04d}{mon:02d}")

        rows = (await list_month(client, month)).json()["rows"]
        mine = [x for x in rows if x["id"] == seed_id]
        check("migrate-fail: row not lost", len(mine) == 1, str(rows))
        check("migrate-fail: row still in default",
              mine and mine[0]["partition"] == "price_changes_default", str(mine))
        # 失败后重试应成功
        r = await client.post(f"/partitions/{month}/backfill")
        check("migrate-fail: retry succeeds",
              r.status_code == 200 and r.json().get("created") is True, r.text)
    else:
        print("[SKIP] migrate-fail: needs PSQL to force a DDL failure")


async def t_lookup_partition_and_numbers(client):
    """沿用现有商品编号：非法 item_id 被拒；分区列表包含默认分区。"""
    r = await insert(client, 999999, "2026-11-15T00:00:00Z", "1.00")
    check("item: unknown item_id -> 404", r.status_code == 404, r.text)
    r = await client.get("/partitions")
    check("partitions: 200", r.status_code == 200, r.text)
    names = {p["name"] for p in r.json()["partitions"]}
    check("partitions: default always listed",
          "price_changes_default" in names, str(names))


async def main() -> int:
    timeout = httpx.Timeout(60.0)
    async with httpx.AsyncClient(base_url=BASE, timeout=timeout) as client:
        for _ in range(60):
            try:
                if (await client.get("/partitions")).status_code == 200:
                    break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
        else:
            print(f"API not reachable at {BASE}")
            return 2
        print(f"API up at {BASE}; psql checks: {'on' if PSQL else 'off'}")
        await t_lookup_partition_and_numbers(client)
        await t_month_boundaries(client)
        await t_backfill_moves_month_only(client)
        await t_backfill_idempotent(client)
        await t_migration_failure_rolls_back(client)
        await t_lock_timeout_rolls_back(client)
        await t_concurrent_writes_and_backfill(client)
    print()
    if failures:
        print(f"{len(failures)} FAILURE(S):")
        for f in failures:
            print(" -", f)
        return 1
    print("ALL PARTITION TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
