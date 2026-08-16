import { AlertCircle, X } from "lucide-react";
import type { PlaceKey } from "../model";
import { useCall } from "./call-store";
import { CALL_FAILURE_MESSAGE } from "./model";

export function CallFailureNotice({ placeKey }: { placeKey: PlaceKey }) {
  const failure = useCall((state) => state.failure);
  const failurePlaceKey = useCall((state) => state.failurePlaceKey);
  const dismiss = useCall((state) => state.dismissFailure);
  if (!failure || failurePlaceKey !== placeKey) return null;
  return (
    <div
      role="alert"
      className="flex items-start gap-2 border-border/70 border-b bg-amber-500/10 px-4 py-2 text-[12px] sm:px-5"
    >
      <AlertCircle className="mt-0.5 size-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
      <p className="min-w-0 flex-1 text-foreground/85">
        {CALL_FAILURE_MESSAGE[failure]}
      </p>
      <button
        type="button"
        aria-label="通話の案内を閉じる"
        onClick={dismiss}
        className="rounded p-0.5 text-muted-foreground hover:bg-accent hover:text-foreground"
      >
        <X className="size-3.5" />
      </button>
    </div>
  );
}
