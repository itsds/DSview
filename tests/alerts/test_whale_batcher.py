"""Tests for alerts/whale_batcher.py: whales within 10 s of each other share a message."""

from __future__ import annotations

from whale_batcher import GAP_SECONDS, MAX_HOLD_SECONDS, WhaleBatcher

T0 = 1_763_633_817.0  # a trade time, Unix seconds


def test_the_decision_is_ten_seconds():
    assert GAP_SECONDS == 10


def test_a_lone_whale_is_due_once_ten_seconds_pass_without_another():
    batcher = WhaleBatcher()
    assert batcher.add("a", T0, now=100.0) == []

    assert batcher.due_in(now=105.0) == 5.0
    assert batcher.pop_due(now=109.9) == []
    assert batcher.pop_due(now=110.0) == ["a"]
    assert batcher.due_in(now=110.0) is None


def test_whales_within_ten_seconds_of_each_other_share_a_group():
    batcher = WhaleBatcher()
    batcher.add("a", T0, now=100.0)
    batcher.add("b", T0 + 4, now=104.0)
    batcher.add("c", T0 + 13, now=113.0)  # 9 s after b, 13 s after a: still "within 10 s of each other"

    assert batcher.pop_due(now=122.9) == []  # c joined at 113, so not due before 123
    assert batcher.pop_due(now=123.0) == ["a", "b", "c"]


def test_a_whale_more_than_ten_seconds_later_closes_the_group_and_starts_the_next():
    batcher = WhaleBatcher()
    batcher.add("a", T0, now=100.0)
    assert batcher.add("b", T0 + 10.5, now=108.0) == [["a"]]  # trade time decides, not arrival
    assert batcher.pending == ["b"]


def test_a_backlog_read_at_once_groups_by_trade_time():
    # After a restart, an hour of alerts can arrive in the same second.
    batcher = WhaleBatcher()
    closed = []
    for i, traded in enumerate([T0, T0 + 3, T0 + 600, T0 + 1200, T0 + 1205]):
        closed += batcher.add(i, traded, now=100.0)
    assert closed == [[0, 1], [2]]
    assert batcher.pending == [3, 4]


def test_an_out_of_order_whale_joins_its_group():
    batcher = WhaleBatcher()
    batcher.add("late", T0 + 5, now=100.0)
    assert batcher.add("early", T0, now=100.1) == []
    assert batcher.pending == ["late", "early"]


def test_a_long_burst_is_still_sent_every_minute():
    batcher = WhaleBatcher()
    now, traded, sent = 100.0, T0, []
    while now < 100.0 + MAX_HOLD_SECONDS + 5:
        batcher.add(now, traded, now=now)  # a whale every 5 s, forever
        sent += [batcher.pop_due(now)]
        now, traded = now + 5, traded + 5
    groups = [group for group in sent if group]
    assert len(groups) == 1
    assert groups[0][0] == 100.0 and len(groups[0]) == MAX_HOLD_SECONDS / 5 + 1


def test_a_full_group_closes_at_once():
    batcher = WhaleBatcher(max_whales=3)
    batcher.add(1, T0, now=100.0)
    batcher.add(2, T0, now=100.0)
    assert batcher.add(3, T0, now=100.0) == [[1, 2, 3]]
    assert batcher.due_in(now=100.0) is None


def test_whales_without_a_trade_time_join_on_arrival():
    batcher = WhaleBatcher()
    batcher.add("a", T0, now=100.0)
    assert batcher.add("b", None, now=101.0) == []
    assert batcher.pop() == ["a", "b"]
    assert batcher.pop() == []
