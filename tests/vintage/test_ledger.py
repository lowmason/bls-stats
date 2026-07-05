from datetime import UTC, date, datetime

from bls_stats.storage.delta import VintageStore
from bls_stats.vintage.ledger import Ledger, SlotRecord


def ts(hour: int) -> datetime:
    return datetime(2026, 7, 2, hour, tzinfo=UTC)


def rec(
    status: str,
    hour: int,
    *,
    revision: int | None = 0,
    benchmark: int | None = 0,
    ref: date = date(2026, 6, 12),
    rel: date = date(2026, 7, 2),
) -> SlotRecord:
    return SlotRecord("ces", ref, rel, revision, benchmark, "increment", 100, status, ts(hour))


def make_ledger(tmp_path) -> Ledger:
    return Ledger(VintageStore(str(tmp_path / "store")))


def test_append_only_status_resolution(tmp_path) -> None:
    led = make_ledger(tmp_path)
    led.record([rec("deferred", 9)])
    led.record([rec("ingested", 11)])  # transition = append, latest wins (ARCH §4.5)
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0) == "ingested"
    assert led.resolved().height == 1


def test_slot_status_none_when_absent(tmp_path) -> None:
    led = make_ledger(tmp_path)
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, 0) is None


def test_null_counters_resolve_null_safely(tmp_path) -> None:
    led = make_ledger(tmp_path)
    led.record([rec("ingested", 9, revision=None, benchmark=None)])
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), None, None) == "ingested"
    assert led.slot_status("ces", date(2026, 6, 12), date(2026, 7, 2), 0, None) is None


def test_prior_benchmark_count(tmp_path) -> None:
    led = make_ledger(tmp_path)
    assert led.prior_benchmark_count("ces", date(2020, 3, 12)) == 0  # null base → 0
    led.record(
        [rec("ingested", 9, ref=date(2020, 3, 12), revision=2, benchmark=1, rel=date(2025, 2, 7))]
    )
    led.record(
        [rec("deferred", 10, ref=date(2020, 3, 12), revision=2, benchmark=2, rel=date(2026, 2, 11))]
    )  # deferred does NOT count
    assert led.prior_benchmark_count("ces", date(2020, 3, 12)) == 1
