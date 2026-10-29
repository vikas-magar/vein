import { Component, type ReactNode } from "react";

/** A rendering failure changes only the editor presentation, never the query or engine session. */
export default class EditorBoundary extends Component<{
  children: ReactNode; fallback: ReactNode; onError: () => void;
}> {
  state = { failed: false };
  static getDerivedStateFromError() { return { failed: true }; }
  componentDidCatch(error: Error) {
    this.props.onError();
    console.error("Vein editor unavailable:", error.message);
  }
  render() {
    if (!this.state.failed) return this.props.children;
    return this.props.fallback;
  }
}
