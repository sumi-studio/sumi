import type { PlaceKey } from "./model";
import { participantKey } from "./model";
import { useMessaging } from "./store";

export interface PlaceDisplay {
  kind: "channel" | "dm" | "group_dm" | "thread";
  name: string;
  topic: string;
}

/** placeの表示名を解決する。DMは相手の表示名（scope-localな名前であってIDではない）。 */
export function usePlaceDisplay(key: PlaceKey | null): PlaceDisplay | null {
  const channels = useMessaging((state) => state.channels);
  const dms = useMessaging((state) => state.dms);
  const threadsById = useMessaging((state) => state.threadsById);
  const membersByKey = useMessaging((state) => state.membersByKey);
  const selfKey = useMessaging((state) => state.selfKey);
  if (!key) return null;
  const channel = channels.find(
    (entry) => `channel:${entry.channelId}` === key,
  );
  if (channel) {
    return { kind: "channel", name: channel.name, topic: channel.topic };
  }
  if (key.startsWith("thread:")) {
    const thread = threadsById[key.slice("thread:".length)];
    if (thread) return { kind: "thread", name: thread.name, topic: "" };
  }
  const dm = dms.find((entry) => `${entry.kind}:${entry.dmId}` === key);
  if (dm) {
    const others = dm.participants
      .filter((ref) => participantKey(ref) !== selfKey)
      .map((ref) => membersByKey[participantKey(ref)]?.displayName ?? "不明");
    return { kind: dm.kind, name: others.join("、"), topic: "" };
  }
  return null;
}
