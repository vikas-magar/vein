/** Dotless glyph lets the brand accent color only the dot, without changing the letter stem. */
export default function Wordmark() {
  return <span className="vein-wordmark" role="img" aria-label="vein">
    <span aria-hidden="true">ve<span className="vein-wordmark-i">ı</span>n</span>
  </span>;
}
