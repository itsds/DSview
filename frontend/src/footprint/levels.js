/**
 * levels.js
 *
 * The footprint's pure functions: price levels merged into rows, minutes
 * combined into bars, the row size that fits the zoom, a bar's point of
 * control and imbalances, and how cell volumes are written. No chart, no
 * network: FootprintStore.js, FootprintLayer.js and the volume profile
 * tools build on these, and Vitest tests them directly.
 *
 * A level is { price, buy, sell }: `price` is the row's lower bound, `buy`
 * the taker volume that lifted the ask there, `sell` the volume that hit
 * the bid. The backend sends them as [price, buy, sell] decimal strings
 * (backend/routers/footprint.py); parseLevels() turns those into numbers.
 *
 * Author: @DS
 */

// Must match config/settings.py's FOOTPRINT_PRICE_STEP and
// FOOTPRINT_TIMEFRAMES (a Vitest test checks): footprint_1m stores levels
// this far apart, on these timeframes.
export const FOOTPRINT_PRICE_STEP = 1;
export const FOOTPRINT_TIMEFRAMES = ["1m", "5m", "15m", "1h"];

// Row sizes the automatic choice picks from, in dollars (each a multiple of
// the step), and the manual choices in the footprint's settings
// (TradingView's "ticks per row").
export const ROW_SIZES = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000];

// A row needs this many pixels to be told apart; with fewer even at the
// largest size, the footprint isn't drawn.
export const MIN_ROW_PX = 14;

// Buy at a row against sell one row below (and sell against buy one row
// above): at this ratio or more, the bigger side is an imbalance — the
// diagonal comparison footprint charts use, since an aggressive buyer
// lifts the ask one tick above where an aggressive seller hits the bid.
export const IMBALANCE_RATIO = 3;

/** [price, buy, sell] arrays (strings or numbers) as { price, buy, sell } numbers. */
export function parseLevels(levels) {
  return levels.map(([price, buy, sell]) => ({ price: Number(price), buy: Number(buy), sell: Number(sell) }));
}

// floor(price / size) * size, without float drift (97000.00000001).
function rowOf(price, size) {
  return Math.round(Math.floor(price / size + 1e-9) * size * 1e8) / 1e8;
}

/** Levels merged into rows `size` wide, prices ascending. */
export function mergeLevels(levels, size) {
  const rows = new Map();
  for (const { price, buy, sell } of levels) {
    const row = rowOf(price, size);
    const cell = rows.get(row);
    if (cell) {
      cell.buy += buy;
      cell.sell += sell;
    } else {
      rows.set(row, { price: row, buy, sell });
    }
  }
  return [...rows.values()].sort((a, b) => a.price - b.price);
}

/** Several minutes' levels (each a list) summed level by level, prices ascending. */
export function combineLevels(lists) {
  return mergeLevels(lists.flat(), FOOTPRINT_PRICE_STEP);
}

/**
 * The row size to draw at: `manual` when it's a number, otherwise the
 * smallest of ROW_SIZES whose rows are at least MIN_ROW_PX tall at
 * `pixelsPerDollar`. Null if even the largest is too thin.
 */
export function pickRowSize(pixelsPerDollar, manual = "auto") {
  if (typeof manual === "number") return manual;
  if (!(pixelsPerDollar > 0)) return null;
  return ROW_SIZES.find((size) => size * pixelsPerDollar >= MIN_ROW_PX) ?? null;
}

/**
 * A bar's summary from its rows (mergeLevels() at `rowSize`): the point of
 * control (the row with the most volume, the lower one on a tie), its delta
 * (buy − sell), total and biggest single cell, and which rows' buy and sell
 * are imbalances.
 */
export function barStats(rows, rowSize) {
  const adjacent = rowSize * (1 + 1e-9); // neighbours one row apart; a gap means nothing traded between
  let poc = null;
  let pocVolume = -1;
  let delta = 0;
  let total = 0;
  let maxCell = 0;
  const buyImbalance = new Set();
  const sellImbalance = new Set();
  rows.forEach((row, i) => {
    const volume = row.buy + row.sell;
    if (volume > pocVolume) {
      poc = row.price;
      pocVolume = volume;
    }
    delta += row.buy - row.sell;
    total += volume;
    maxCell = Math.max(maxCell, row.buy, row.sell);
    const below = rows[i - 1];
    const above = rows[i + 1];
    if (below && row.price - below.price <= adjacent && below.sell > 0 && row.buy >= IMBALANCE_RATIO * below.sell) {
      buyImbalance.add(row.price);
    }
    if (above && above.price - row.price <= adjacent && above.buy > 0 && row.sell >= IMBALANCE_RATIO * above.buy) {
      sellImbalance.add(row.price);
    }
  });
  return { poc, delta, total, maxCell, buyImbalance, sellImbalance };
}

/**
 * A volume profile's value area: the rows around the point of control
 * holding `share` (70%) of the volume, grown the usual way — one row at a
 * time towards whichever neighbour, above or below, traded more. Returns
 * { poc, low, high }: the POC row's price and the lowest and highest rows'
 * prices (lower bounds); null for no rows.
 */
export function valueArea(rows, share = 0.7) {
  if (rows.length === 0) return null;
  const volumes = rows.map((row) => row.buy + row.sell);
  const total = volumes.reduce((sum, v) => sum + v, 0);
  let pocIndex = 0;
  volumes.forEach((volume, i) => {
    if (volume > volumes[pocIndex]) pocIndex = i;
  });
  let low = pocIndex;
  let high = pocIndex;
  let inside = volumes[pocIndex];
  while (inside < share * total && (low > 0 || high < rows.length - 1)) {
    const below = low > 0 ? volumes[low - 1] : -1;
    const above = high < rows.length - 1 ? volumes[high + 1] : -1;
    if (above >= below) inside += volumes[++high];
    else inside += volumes[--low];
  }
  return { poc: rows[pocIndex].price, low: rows[low].price, high: rows[high].price };
}

const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

/** A cell's volume in at most ~4 characters: 0, 0.42, 3.17, 12.5, 128, 1.2K. */
export function formatVolume(value) {
  const abs = Math.abs(value);
  if (abs === 0) return "0";
  if (abs < 0.01) return value < 0 ? "-<.01" : "<.01";
  if (abs < 10) return trimZeros(value.toFixed(2));
  if (abs < 100) return trimZeros(value.toFixed(1));
  if (abs < 1000) return value.toFixed(0);
  return compact.format(value);
}

function trimZeros(text) {
  return text.includes(".") ? text.replace(/\.?0+$/, "") : text;
}
