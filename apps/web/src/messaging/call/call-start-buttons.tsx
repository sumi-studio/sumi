import { Phone, PhoneCall } from "lucide-react";
import type { PlaceKey } from "../model";
import { isCallActive, useCall } from "./call-store";

export function CallStartButtons({ placeKey }: { placeKey: PlaceKey }) {
  const activePlaceKey = useCall((state) => state.activePlaceKey);
  const phase = useCall((state) => state.phase);
  const active = useCall((state) => isCallActive(state, placeKey));
  const join = useCall((state) => state.join);
  const here = activePlaceKey === placeKey;
  if (here && phase !== "failed") return null;
  return (
    <button
      type="button"
      title={active ? "通話に参加" : "通話を開始"}
      aria-label={active ? "通話に参加" : "通話を開始"}
      onClick={() => void join(placeKey)}
      className="flex size-8 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
    >
      {active ? <PhoneCall className="size-4" /> : <Phone className="size-4" />}
    </button>
  );
}
