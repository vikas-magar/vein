"use client";

import { usePathname } from "next/navigation";
import SiteHeader from "./site-header";

export default function SiteShell({ children }) {
  const workspace = usePathname() === "/";
  return <>
    {!workspace && <SiteHeader />}
    <main className={workspace ? "workspace-main" : "site-main"}>{children}</main>
    {!workspace && <footer className="site-footer"><span>VEIN · LOCAL ANALYTICS</span>
      <span>Early alpha · Built for data that stays close</span></footer>}
  </>;
}
