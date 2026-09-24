/**
 * ErrorBoundary.jsx
 *
 * Catches errors thrown while rendering (or in the effects of) whatever it
 * wraps and shows a message with a retry button in its place — otherwise
 * React unmounts the whole app on any uncaught error, leaving a blank
 * page. A class component because React has no hook equivalent of
 * getDerivedStateFromError.
 *
 * Errors in event handlers and async callbacks (a WebSocket onmessage, a
 * chart subscription) never reach a boundary — React doesn't route those
 * through the component tree.
 *
 * Author: @DS
 */

import { Component } from "react";

import styles from "./ErrorBoundary.module.css";

export default class ErrorBoundary extends Component {
  state = { error: null };

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    console.error(`${this.props.name ?? "Component"} crashed:`, error, info.componentStack);
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;

    const name = this.props.name ?? "panel";
    return (
      <div role="alert" className={styles.fallback}>
        <p className={styles.title}>The {name} hit an error</p>
        <p className={styles.message}>{String(error?.message ?? error)}</p>
        {/* Clearing the error remounts the children from scratch. */}
        <button type="button" className={styles.retry} onClick={() => this.setState({ error: null })}>
          Reload {name}
        </button>
      </div>
    );
  }
}
