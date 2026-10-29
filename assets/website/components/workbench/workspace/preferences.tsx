import { useEffect, useState } from "react";
import { Button, Dialog, DialogTrigger, Heading, Modal, ModalOverlay, NumberField, Label, Group, Input } from "react-aria-components";

const FONT_KEY = "vein.editor-font-size";

export default function Preferences({ isOpen, onOpenChange }: {
  isOpen: boolean; onOpenChange: (open: boolean) => void;
}) {
  const [size, setSize] = useState(13);
  function apply(value: number) {
    const next = Number.isFinite(value) ? Math.max(11, Math.min(22, value)) : 13;
    setSize(next);
    document.documentElement.style.setProperty("--editor-font-size", `${next}px`);
    window.dispatchEvent(new Event("vein:editor-font-change"));
    try { localStorage.setItem(FONT_KEY, String(next)); } catch {}
  }
  useEffect(() => {
    try {
      const saved = localStorage.getItem(FONT_KEY);
      if (saved !== null) apply(Number(saved));
    } catch {}
  }, []);
  return <DialogTrigger isOpen={isOpen} onOpenChange={onOpenChange}>
    <Button className="ide-tool ide-settings">Settings</Button>
    <ModalOverlay className="ide-modal-overlay"><Modal className="ide-modal"><Dialog>
      {({ close }) => <><Heading slot="title">Editor settings</Heading>
        <p>Vein dark theme · JetBrains Mono</p>
        <NumberField value={size} onChange={apply} minValue={11} maxValue={22}>
          <Label>Editor font size</Label>
          <Group className="ide-font-setting"><Button slot="decrement" className="ide-tool">−</Button>
            <Input /><Button slot="increment" className="ide-tool">+</Button></Group>
        </NumberField>
        <div className="ide-confirm-actions"><Button className="ide-tool" onPress={() => apply(13)}>Reset</Button>
          <Button className="ide-primary" onPress={close}>Done</Button></div>
      </>}
    </Dialog></Modal></ModalOverlay>
  </DialogTrigger>;
}
