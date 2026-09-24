/**
 * chartTheme.js
 *
 * lightweight-charts draws on a <canvas>, which can't resolve CSS
 * variables, so this reads styles/theme.css's tokens once and turns them
 * into chart and series options — keeping theme.css the only place a
 * colour is defined. The same theme object colours drawing/'s drawings.
 *
 * Author: @DS
 */

import { ColorType, CrosshairMode } from "lightweight-charts";

export function readChartTheme() {
  const styles = getComputedStyle(document.documentElement);
  const token = (name) => styles.getPropertyValue(name).trim();
  return {
    surface: token("--color-surface"),
    surfaceHover: token("--color-surface-hover"),
    border: token("--color-border"),
    grid: token("--color-grid"),
    text: token("--color-text"),
    textMuted: token("--color-text-muted"),
    accent: token("--color-accent"),
    accentSoft: token("--color-accent-soft"),
    onAccent: token("--color-on-accent"),
    up: token("--color-up"),
    down: token("--color-down"),
    upVolume: token("--color-up-volume"),
    downVolume: token("--color-down-volume"),
    upSoft: token("--color-up-soft"),
    downSoft: token("--color-down-soft"),
    warning: token("--color-warning"),
    info: token("--color-info"),
    series: [1, 2, 3].map((n) => token(`--color-series-${n}`)), // indicator colour slots, in order
    fontUi: token("--font-ui"),
  };
}

export function chartOptions(theme, { intraday = true } = {}) {
  return {
    autoSize: true, // follows the container via ResizeObserver, e.g. when side panels collapse
    layout: {
      background: { type: ColorType.Solid, color: theme.surface },
      textColor: theme.textMuted,
      fontFamily: theme.fontUi,
      // lightweight-charts' license (see its README) requires a link to
      // tradingview.com on pages using it, plus its attribution notice; this
      // built-in logo satisfies the link part. Only turn it off after adding
      // that link somewhere else on the page.
      attributionLogo: true,
      panes: {
        separatorColor: theme.border,
        separatorHoverColor: theme.surfaceHover,
        enableResize: true,
      },
    },
    grid: {
      vertLines: { color: theme.grid },
      horzLines: { color: theme.grid },
    },
    crosshair: {
      mode: CrosshairMode.Normal, // moves freely instead of snapping to the close
      vertLine: { color: theme.textMuted, labelBackgroundColor: theme.surfaceHover },
      horzLine: { color: theme.textMuted, labelBackgroundColor: theme.surfaceHover },
    },
    rightPriceScale: { borderColor: theme.border },
    timeScale: {
      borderColor: theme.border,
      timeVisible: intraday, // intraday bars need hh:mm on the axis; daily bars only dates
      secondsVisible: false,
      rightOffset: 8,
    },
  };
}

export function candleSeriesOptions(theme) {
  return {
    upColor: theme.up,
    downColor: theme.down,
    borderUpColor: theme.up,
    borderDownColor: theme.down,
    wickUpColor: theme.up,
    wickDownColor: theme.down,
  };
}
