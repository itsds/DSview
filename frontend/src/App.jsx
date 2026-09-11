/**
 * App.jsx
 *
 * Root component — BTCUSDT candle chart with real historical backfill
 * plus a live-updating tail. Symbol selection and timeframe switching are
 * still future work (see CLAUDE.md's Deployment approach).
 *
 * Author: @DS
 */

import CandleChart from "./components/CandleChart.jsx";
import { useCandleHistory } from "./hooks/useCandleHistory.js";
import { useLiveCandles } from "./hooks/useLiveCandles.js";

const SYMBOL = "BTCUSDT";

export default function App() {
  const history = useCandleHistory(SYMBOL);
  const candle = useLiveCandles(SYMBOL);

  return (
    <div style={{ fontFamily: "sans-serif", padding: "1rem" }}>
      <h1>{SYMBOL}</h1>
      <CandleChart history={history} candle={candle} />
    </div>
  );
}
