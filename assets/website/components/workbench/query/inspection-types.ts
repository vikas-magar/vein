export interface EngineDiagnostic {
  message: string; category: string; from: number | null; to: number | null;
}
export interface Inspection {
  columns: { name: string; qualifier: string | null; data_type: string }[];
  functions: { name: string; signature: string }[];
  diagnostics: EngineDiagnostic[];
}
