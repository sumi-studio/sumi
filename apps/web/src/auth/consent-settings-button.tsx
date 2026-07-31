import { useState } from "react";
import { ConsentScreen } from "./consent-screen";
import { useResearchConsent } from "./use-research-consent";

/**
 * A small, always-available entry point to review and change the 研究協力
 * decision (ADR 0009 §6). The first-time request is shown by AuthGate; this
 * control satisfies the "changeable later" contract for already-decided Humans.
 */
export function ConsentSettingsButton() {
  const [open, setOpen] = useState(false);
  const consent = useResearchConsent(true);

  if (open) {
    return (
      <ConsentScreen
        onChangeComplete={() => {
          setOpen(false);
          void consent.refresh();
        }}
      />
    );
  }

  return (
    <button
      type="button"
      onClick={() => setOpen(true)}
      className="fixed right-3 bottom-3 z-20 rounded-full bg-background/80 px-3 py-1.5 text-muted-foreground text-xs shadow-sm ring-1 ring-neutral-200 backdrop-blur transition-colors hover:text-foreground dark:ring-neutral-800"
      aria-label="研究協力の設定を変更する"
    >
      研究協力: {consent.granted ? "同意" : "辞退"}
    </button>
  );
}
