import { useEffect, useState, type RefObject } from "react";
import { Button, Menu, MenuItem, MenuTrigger, Popover, RouterProvider } from "react-aria-components";
import { useRouter } from "next/navigation";
import Icon from "../icon";

export type WorkspacePanel = "commands" | "settings";

/** Responsive entry points only; dialog state and action implementations remain shared. */
export default function MobileMenu({ onOpen, triggerRef }: {
  onOpen: (panel: WorkspacePanel) => void; triggerRef: RefObject<HTMLButtonElement | null>;
}) {
  const [open, setOpen] = useState(false);
  const router = useRouter();
  useEffect(() => {
    const narrow = window.matchMedia("(max-width: 760px)");
    const changed = () => { if (!narrow.matches) setOpen(false); };
    narrow.addEventListener("change", changed);
    return () => narrow.removeEventListener("change", changed);
  }, []);
  return <RouterProvider navigate={href => router.push(href)}>
    <MenuTrigger isOpen={open} onOpenChange={setOpen}>
      <Button ref={triggerRef} className="ide-mobile-menu" aria-label="Workspace menu"><Icon name="menu" size={18} /></Button>
      <Popover className="ide-mobile-popover" placement="bottom end" offset={6}>
        <Menu aria-label="Workspace menu">
          <MenuItem id="commands" className="ide-mobile-menu-item" onAction={() => onOpen("commands")}>
            <Icon name="search" size={15} />Commands
          </MenuItem>
          <MenuItem id="settings" className="ide-mobile-menu-item" onAction={() => onOpen("settings")}>
            <Icon name="settings" size={15} />Settings
          </MenuItem>
          <MenuItem id="documentation" className="ide-mobile-menu-item" href="/docs/">
            <Icon name="book" size={15} />Documentation
          </MenuItem>
        </Menu>
      </Popover>
    </MenuTrigger>
  </RouterProvider>;
}
