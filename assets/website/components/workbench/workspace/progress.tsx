import { Label, ProgressBar } from "react-aria-components";
import { useEngineSnapshot } from "./hooks";

/** Only registration has a known total. Query completion is intentionally indeterminate. */
export default function ExecutionProgress() {
  const engine = useEngineSnapshot();
  if (engine.phase !== "loading" && engine.phase !== "running") return null;
  const progress = engine.progress;
  const determinate = progress?.stage === "registering" && Number.isFinite(progress.total)
    && Number.isFinite(progress.completed) && progress.total! > 0;
  const total = determinate ? progress!.total! : 1;
  const value = determinate ? Math.max(0, Math.min(total, progress!.completed!)) : undefined;
  return <ProgressBar className="ide-execution-progress" isIndeterminate={!determinate}
    value={value} maxValue={total}>
    {({ percentage }) => <>
      <svg className="ide-progress-pulse" viewBox="40 76 176 112" aria-hidden="true">
        <path className="ide-pulse-base" d="M48 128H92L108 88L136 176L160 128H208" />
        <path className="ide-pulse-flow" pathLength="100" d="M48 128H92L108 88L136 176L160 128H208" />
      </svg>
      <Label className="ide-progress-label">{engine.message}</Label>
      {determinate && <span className="ide-progress-percent" aria-hidden="true">{Math.round(percentage ?? 0)}%</span>}
    </>}
  </ProgressBar>;
}
