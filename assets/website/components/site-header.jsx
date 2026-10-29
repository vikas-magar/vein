import Link from "next/link";
import Image from "next/image";
import { assetUrl } from "./workbench/assets.mjs";
import Wordmark from "./wordmark";

const links = [
  ["Docs", "/docs/"],
];

export default function SiteHeader() {
  return (
    <header className="site-header">
      <Link className="brand" href="/" aria-label="Vein home">
        <Image className="brand-mark" src={assetUrl("vein.svg")} alt="" width={30} height={30} /><Wordmark />
      </Link>
      <nav aria-label="Main navigation">
        {links.map(([label, href]) => <Link key={href} href={href}>{label}</Link>)}
      </nav>
      <Link className="header-cta" href="/">Open workspace <span aria-hidden="true">↗</span></Link>
    </header>
  );
}
