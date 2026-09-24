/**
 * RightPanel.jsx
 *
 * Watchlist and whale-trade feed down the right side. Only one symbol is
 * ingested so far (ingestion/main.py's SYMBOLS), so the watchlist is a
 * single row priced off the same live candle stream as the chart. The
 * whale feed stays empty until whale_detector.py publishes its flagged
 * trades somewhere the backend can serve them from — it only logs today.
 *
 * Author: @DS
 */

import { Plus } from "lucide-react";

import styles from "./RightPanel.module.css";
import ToolbarButton from "./ToolbarButton.jsx";

export default function RightPanel({ symbol, candle }) {
  const last = candle ? Number(candle.close).toFixed(2) : "—";

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
        </div>
        <p className={styles.empty}>Not connected yet.</p>
      </section>
    </aside>
  );
}
