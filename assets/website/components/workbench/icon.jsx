const paths = {
  folder: "M3 7V5a1 1 0 0 1 1-1h5l2 2h9a1 1 0 0 1 1 1v11a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V7Z",
  file: "M14 3H5v18h14V8l-5-5Zm0 0v5h5M8 12h8M8 16h5",
  table: "M3 4h18v16H3V4Zm0 5h18M8 9v11",
  play: "m8 5 11 7-11 7V5Z",
  stop: "M6 6h12v12H6V6Z",
  search: "M21 21l-5-5M18 10.5a7.5 7.5 0 1 1-15 0 7.5 7.5 0 0 1 15 0Z",
  download: "M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5",
  menu: "M4 6h16M4 12h16M4 18h16",
  settings: "M4 6h16M4 12h16M4 18h16M8 4v4M16 10v4M10 16v4",
  book: "M12 5C8 2 3 4 3 4v15s5-2 9 1c4-3 9-1 9-1V4s-5-2-9 1Zm0 0v15",
  arrow: "M5 12h14m-6-6 6 6-6 6",
  close: "m6 6 12 12M6 18 18 6",
  database: "M20 6c0 2-4 3-8 3S4 8 4 6s4-3 8-3 8 1 8 3Zm0 0v12c0 2-4 3-8 3s-8-1-8-3V6m0 6c0 2 4 3 8 3s8-1 8-3",
};

export default function Icon({ name, size = 16, className = "" }) {
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor"
    strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" className={className} aria-hidden="true">
    <path d={paths[name] ?? paths.file} />
  </svg>;
}
