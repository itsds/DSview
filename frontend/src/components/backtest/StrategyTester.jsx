/**
 * StrategyTester.jsx
 *
 * The backtest results panel under the chart (Phase 4b's kickoff decision
 * 2), opened from the bottom bar: pick a run (every run is kept, newest
 * first), then its Overview (the stats and the equity curve) or its Trades
 * (the list, newest first). Runs come from the command line
 * (backtest/run_backtest.py); the panel only reads them, through
 * hooks/useBacktests.js, which App.jsx owns so the chart can mark the
 * picked run's trades ("Trades on chart").
 *
 * Author: @DS
 */

import { useState } from "react";
import { RefreshCw, X } from "lucide-react";

import {
  formatCount,
  formatMoney,
  formatPct,
  formatPrice,
  formatRatio,
  formatSignedMoney,
  formatTime,
  runLabel,
  sparkCheckNote,
} from "../../backtest/format.js";
import { DAY_SECONDS, timeframeSeconds } from "../../lib/timeframes.js";
import ToolbarButton from "../layout/ToolbarButton.jsx";
import EquityChart from "./EquityChart.jsx";
import styles from "./StrategyTester.module.css";

const TABS = [
  { id: "overview", label: "Overview" },
  { id: "trades", label: "List of trades" },
];
// Rows the trade list renders at most; a year of 1m bars can make thousands.
const MAX_ROWS = 1000;

const tone = (value) => (value > 0 ? styles.up : value < 0 ? styles.down : undefined);

function Stat({ label, value, detail, valueClass }) {
  return (
    <div className={styles.stat}>
      <span className={styles.statLabel}>{label}</span>
      <span className={`${styles.statValue} ${valueClass ?? ""}`}>{value}</span>
      {detail && <span className={styles.statDetail}>{detail}</span>}
    </div>
  );
}

function Overview({ run, equity }) {
  const s = run.stats;
  const check = sparkCheckNote(run.sparkCheck);
  return (
    <div className={styles.overview}>
      <div className={styles.stats}>
        <Stat
          label="Net profit"
          value={formatSignedMoney(s.net_profit)}
          detail={formatPct(s.net_profit_pct, { signed: true })}
          valueClass={tone(s.net_profit)}
        />
        <Stat
          label="Total trades"
          value={formatCount(s.total_trades)}
          detail={`${formatCount(s.long_trades)} long · ${formatCount(s.short_trades)} short`}
        />
        <Stat
          label="Win rate"
          value={formatPct(s.win_rate_pct)}
          detail={`${formatCount(s.winning_trades)} won · ${formatCount(s.losing_trades)} lost`}
        />
        <Stat label="Profit factor" value={formatRatio(s.profit_factor)} />
        <Stat
          label="Max drawdown"
          value={formatMoney(s.max_drawdown)}
          detail={formatPct(s.max_drawdown_pct)}
          valueClass={s.max_drawdown > 0 ? styles.down : undefined}
        />
        <Stat label="Avg trade" value={formatSignedMoney(s.avg_trade)} valueClass={tone(s.avg_trade)} />
        <Stat label="Fees" value={formatMoney(s.total_fees)} />
        <Stat
          label="Buy & hold"
          value={formatPct(s.buy_and_hold_pct, { signed: true })}
          valueClass={tone(s.buy_and_hold_pct)}
        />
      </div>
      <div className={styles.curve}>
        {equity ? (
          <EquityChart
            key={run.id}
            points={equity.points}
            capital={run.capital}
            intraday={timeframeSeconds(run.timeframe) < DAY_SECONDS}
          />
        ) : (
          <p className={styles.empty}>Loading the equity curve…</p>
        )}
      </div>
      <p className={styles.footnote}>
        <span className={check.ok === true ? styles.up : check.ok === false ? styles.down : undefined}>
          {check.text}
        </span>
        {run.sameAs !== null && <span> · same result as run #{run.sameAs}</span>}
        <span>
          {" "}
          · {formatMoney(run.capital)} all in, {formatPct(run.feeRate * 100)} fee per side · {formatCount(s.bars)} bars
          from {formatTime(run.firstBar)} to {formatTime(run.lastBar)} UTC
        </span>
      </p>
    </div>
  );
}

function TradeList({ trades }) {
  if (!trades) return <p className={styles.empty}>Loading the trades…</p>;
  if (trades.length === 0) return <p className={styles.empty}>This run made no trades.</p>;
  const rows = trades.slice(-MAX_ROWS).reverse();
  return (
    <div className={styles.tableWrap}>
      {trades.length > MAX_ROWS && (
        <p className={styles.empty}>
          The newest {formatCount(MAX_ROWS)} of {formatCount(trades.length)} trades.
        </p>
      )}
      <table className={styles.table}>
        <thead>
          <tr>
            <th>#</th>
            <th>Type</th>
            <th>Entry (UTC)</th>
            <th className={styles.numeric}>Entry price</th>
            <th>Exit (UTC)</th>
            <th className={styles.numeric}>Exit price</th>
            <th className={styles.numeric}>Net P&amp;L</th>
            <th className={styles.numeric}>Return</th>
            <th className={styles.numeric}>Bars</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((trade) => (
            <tr key={trade.number}>
              <td>{trade.number}</td>
              <td className={trade.side === "long" ? styles.up : styles.down}>
                {trade.side === "long" ? "Long" : "Short"}
              </td>
              <td>{formatTime(trade.entryTime)}</td>
              <td className={styles.numeric}>{formatPrice(trade.entryPrice)}</td>
              <td title={trade.closedByEnd ? "Still open when the data ended: closed at the last bar's close" : undefined}>
                {formatTime(trade.exitTime)}
                {trade.closedByEnd && " (end)"}
              </td>
              <td className={styles.numeric}>{formatPrice(trade.exitPrice)}</td>
              <td className={`${styles.numeric} ${tone(trade.pnl) ?? ""}`}>{formatSignedMoney(trade.pnl)}</td>
              <td className={`${styles.numeric} ${tone(trade.returnPct) ?? ""}`}>
                {formatPct(trade.returnPct, { signed: true })}
              </td>
              <td className={styles.numeric}>{formatCount(trade.bars)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function StrategyTester({ backtests, onClose }) {
  const [tab, setTab] = useState("overview");
  const { runs, error, picked, trades, equity, showTrades, select, setShowTrades, refresh } = backtests;

  let body;
  if (runs === null) {
    body = <p className={styles.empty}>{error ? `Couldn't load the backtests: ${error}` : "Loading…"}</p>;
  } else if (!picked) {
    body = (
      <p className={styles.empty}>
        No backtests yet. Run one from the command line:{" "}
        <code>cd backtest &amp;&amp; python run_backtest.py --strategy ema_cross</code>
      </p>
    );
  } else if (tab === "overview") {
    body = <Overview run={picked} equity={equity} />;
  } else {
    body = <TradeList trades={trades} />;
  }

  return (
    <section className={styles.panel} aria-label="Strategy Tester">
      <header className={styles.header}>
        <h2 className={styles.heading}>Strategy Tester</h2>
        {runs && runs.length > 0 && (
          <select
            className={styles.select}
            value={picked?.id ?? ""}
            onChange={(event) => select(Number(event.target.value))}
            aria-label="Backtest run"
          >
            {runs.map((run) => (
              <option key={run.id} value={run.id}>
                {runLabel(run)}
              </option>
            ))}
          </select>
        )}
        <nav className={styles.tabs} role="tablist">
          {TABS.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={tab === t.id}
              className={tab === t.id ? `${styles.tab} ${styles.activeTab}` : styles.tab}
              onClick={() => setTab(t.id)}
            >
              {t.label}
            </button>
          ))}
        </nav>
        <div className={styles.spacer} />
        <label className={styles.toggle}>
          <input type="checkbox" checked={showTrades} onChange={(event) => setShowTrades(event.target.checked)} />
          Trades on chart
        </label>
        <ToolbarButton icon={RefreshCw} title="Reload the runs" onClick={refresh} />
        <ToolbarButton icon={X} title="Close the Strategy Tester" onClick={onClose} />
      </header>
      <div className={styles.body}>{body}</div>
    </section>
  );
}
