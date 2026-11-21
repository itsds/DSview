"""
whale_batcher.py

Groups whales that trade close together into one Telegram message, per
Phase 1's kickoff decision (docs/ROADMAP.md): whales within 10 seconds of
each other are combined instead of notifying once each.

"Within 10 seconds of each other" is a gap rule on the trades' own
timestamps (event_time): a whale joins the open group if it traded no
more than GAP after the group's latest whale, so a burst of whales 5 s
apart stays one group however long it runs. Grouping on trade time rather
than on arrival time keeps a backlog read after a restart grouped the way
it happened live, instead of collapsing an hour of whales into one
message because they all arrived in the same second.

A group is closed, ready to send, when any of these happens. Live, it
takes a clock as well as trade times, since nothing says whether another
whale is coming:

- a whale arrives that doesn't fit: the group closes and the newcomer
  starts the next one;
- GAP seconds pass (wall clock) with no whale joining: anything that
  trades from now on is more than GAP after the group, so can't join;
- the group has been open MAX_HOLD seconds since its first whale, so a
  long burst still notifies every minute rather than only once it ends;
- it holds MAX_WHALES, which keeps a message far below Telegram's
  4096-character limit (a card is under 100 characters).

So a lone whale reaches the phone about GAP seconds after it trades: the
price of combining at all.

Pure and clock-agnostic: the caller passes `now` (seconds, any monotonic
clock) and each item's trade time (Unix seconds, or None if unknown),
and gets back closed groups to send. Items are opaque: the bot passes
(partition, offset, alert) so it knows which offsets a sent group covers.

Author: @DS
"""

from __future__ import annotations

from typing import Generic, List, Optional, TypeVar

Item = TypeVar("Item")

# Phase 1's kickoff decision.
GAP_SECONDS = 10.0
MAX_HOLD_SECONDS = 60.0
MAX_WHALES = 20


class WhaleBatcher(Generic[Item]):
    """Collects whales into groups; see the module docstring for when a group is due."""

    def __init__(
        self,
        gap_seconds: float = GAP_SECONDS,
        max_hold_seconds: float = MAX_HOLD_SECONDS,
        max_whales: int = MAX_WHALES,
    ) -> None:
        self._gap = gap_seconds
        self._max_hold = max_hold_seconds
        self._max_whales = max_whales
        self._items: List[Item] = []
        self._latest_trade: Optional[float] = None  # newest trade time in the open group
        self._opened_at = 0.0  # `now` when the open group got its first whale
        self._joined_at = 0.0  # `now` when its latest whale joined

    @property
    def pending(self) -> List[Item]:
        """The open group's items, oldest first (not yet due)."""
        return list(self._items)

    def add(self, item: Item, traded_at: Optional[float], now: float) -> List[List[Item]]:
        """Add one whale; return the groups this closes (none, one, or two), oldest first."""
        closed: List[List[Item]] = []
        if self._items and not self._fits(traded_at):
            closed.append(self.pop())

        if not self._items:
            self._opened_at = now
        self._items.append(item)
        self._joined_at = now
        if traded_at is not None:
            self._latest_trade = traded_at if self._latest_trade is None else max(self._latest_trade, traded_at)

        if len(self._items) >= self._max_whales:
            closed.append(self.pop())
        return closed

    def due_in(self, now: float) -> Optional[float]:
        """Seconds until the open group is due (0 if it already is), or None with no group open."""
        if not self._items:
            return None
        deadline = min(self._joined_at + self._gap, self._opened_at + self._max_hold)
        return max(0.0, deadline - now)

    def pop_due(self, now: float) -> List[Item]:
        """The open group if it's due, closing it; else an empty list."""
        due_in = self.due_in(now)
        return self.pop() if due_in is not None and due_in <= 0 else []

    def pop(self) -> List[Item]:
        """Close the open group now, due or not (shutdown), and return it; [] if none is open."""
        items, self._items = self._items, []
        self._latest_trade = None
        return items

    def _fits(self, traded_at: Optional[float]) -> bool:
        # A whale with no usable trade time can't be placed: it joins on
        # arrival alone, like a live whale would.
        if traded_at is None or self._latest_trade is None:
            return True
        return traded_at - self._latest_trade <= self._gap
