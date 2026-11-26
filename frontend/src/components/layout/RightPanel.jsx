/**
 * RightPanel.jsx
 *
 * Watchlist and whale-trade feed down the right side. Only one symbol is
 * ingested so far (config/settings.py's SYMBOLS), so the watchlist is a
 * single row priced off the same live candle stream as the chart. The
 * whale feed lists every symbol's whale trades, newest first, as
 * hooks/useWhaleTrades.js receives them from the backend's /ws/whales;
 * times are UTC, like the chart's time axis and the bottom bar's clock.
 *
 * Author: @DS
 */

import { Plus } from "lucide-react";

import styles from "./RightPanel.module.css";
import ToolbarButton from "./ToolbarButton.jsx";

const STATUS_LABELS = { connecting: "Connecting", live: "Live", reconnecting: "Reconnecting" };

const EMPTY_MESSAGES = {
  connecting: "Connecting…",
  live: "No whale trades yet.",
  reconnecting: "Can't reach the whale feed. Retrying…",
};

// Notional is in the quote currency (USDT for every symbol so far), shown as dollars.
const usd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  notation: "compact",
  maximumFractionDigits: 1,
});

const quantity = new Intl.NumberFormat("en-US", { maximumFractionDigits: 8 });

function utcTime(iso) {
  return iso ? new Date(iso).toISOString().slice(11, 19) : "";
}

export default function RightPanel({ symbol, candle, whaleTrades, whaleStatus, replaying = false }) {
  const last = candle ? Number(candle.close).toFixed(2) : "—";
  const threshold = whaleTrades[0]?.threshold;

  return (
    <aside className={styles.panel}>
      <section className={styles.section} aria-labelledby="watchlist-heading">
        <div className={styles.header}>
          <h2 id="watchlist-heading" className={styles.heading}>
            Watchlist
          </h2>
          <ToolbarButton icon={Plus} title="Add symbol" notBuilt />
        </div>
        <table className={styles.table}>
          <thead>
            <tr>
              <th scope="col">Symbol</th>
              <th scope="col" className={styles.numeric}>
                Last
              </th>
            </tr>
          </thead>
          <tbody>
            <tr className={styles.current} aria-current="true">
              <td>{symbol}</td>
              <td className={styles.numeric}>{last}</td>
            </tr>
          </tbody>
        </table>
      </section>
      <section className={`${styles.section} ${styles.grow}`} aria-labelledby="whales-heading">
        <div className={styles.header}>
          <h2 id="whales-heading" className={styles.heading}>
            Whale trades
          </h2>
          <span className={styles.status} data-status={whaleStatus} role="status">
            {replaying && whaleStatus === "live" ? "Replay" : STATUS_LABELS[whaleStatus]}
          </span>
        </div>
        {threshold && <p className={styles.caption}>{usd.format(threshold)} or more · times in UTC</p>}
        {whaleTrades.length === 0 ? (
          <p className={styles.empty}>{EMPTY_MESSAGES[whaleStatus]}</p>
        ) : (
          <ol className={styles.whales} aria-labelledby="whales-heading">
            {whaleTrades.map((trade) => (
              <WhaleTrade key={`${trade.exchange}:${trade.trade_id}`} trade={trade} />
            ))}
          </ol>
        )}
      </section>
    </aside>
  );
}

function WhaleTrade({ trade }) {
  const isBuy = trade.side === "buy";
  const time = trade.event_time ?? trade.detected_at;
  return (
    <li className={styles.whale}>
      <span className={styles.side} data-side={isBuy ? "buy" : "sell"}>
        {isBuy ? "Buy" : "Sell"}
      </span>
      <span className={styles.notional}>{usd.format(trade.notional)}</span>
      <time className={styles.time} dateTime={time}>
        {utcTime(time)}
      </time>
      <span className={styles.detail}>
        {quantity.format(trade.quantity)} {trade.symbol} @ {Number(trade.price).toFixed(2)}
      </span>
    </li>
  );
}
