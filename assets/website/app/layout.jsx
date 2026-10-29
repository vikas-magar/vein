import "./globals.css";
import localFont from "next/font/local";
import SiteShell from "../components/site-shell";

const mono = localFont({
  src: "../public/fonts/JetBrainsMonoNerdFontMono-Regular.ttf",
  variable: "--font-vein-mono", display: "swap", weight: "400",
});

export const metadata = {
  title: { default: "Vein — local query workspace", template: "%s — Vein" },
  description: "A local analytical engine for querying Parquet with composable pipeline queries.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en" data-scroll-behavior="smooth" className={mono.variable}>
      <body>
        <SiteShell>{children}</SiteShell>
      </body>
    </html>
  );
}
