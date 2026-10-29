import { useId, useState } from "react";
import { Button } from "react-aria-components";
import type { EngineDiagnostic } from "./inspection-types";

/** Only explicit expansion changes layout; diagnostic updates keep the reserved size. */
export default function Problems({ errors, onNavigate }: {
  errors: EngineDiagnostic[]; onNavigate: (from: number, to: number) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const id = useId();
  return <section className="ide-problems" aria-label="Problems">
    <Button className="ide-problems-toggle" aria-expanded={expanded} aria-controls={id}
      onPress={() => setExpanded(value => !value)}>
      <span aria-hidden="true">{expanded ? "−" : "+"}</span> Problems
      <span className={errors.length ? "ide-problem-count" : ""} aria-live="polite">{errors.length}</span>
    </Button>
    <div id={id} hidden={!expanded} className="ide-problems-body" aria-label="Query diagnostics">
      {!errors.length ? <p className="ide-problems-empty">No problems reported for the current statement.</p>
        : <ul>{errors.map((error, index) => <li key={index}>
          {error.from !== null && error.to !== null
            ? <Button className="ide-problem" onPress={() => onNavigate(error.from!, error.to!)}>
              <span>Vein {error.category}</span>{error.message}</Button>
            : <div className="ide-problem"><span>Vein {error.category} · location unavailable</span>{error.message}</div>}
        </li>)}</ul>}
    </div>
  </section>;
}
