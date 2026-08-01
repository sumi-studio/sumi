import { useNavigate } from "@tanstack/react-router";
import { useCallback } from "react";
import { type PlaceKey, parsePlaceKey } from "./model";

/**
 * placeはURLを持つ。channel = /c/:id、DM = /dm/:id、グループDM = /group/:id。
 * リロード・戻る/進む・リンク共有がすべてURL経由で成立する。
 */
export function placePath(key: PlaceKey): string {
  const place = parsePlaceKey(key);
  if (!place) return "/";
  if (place.kind === "channel") return `/c/${place.channelId}`;
  if (place.kind === "dm") return `/dm/${place.dmId}`;
  return `/group/${place.dmId}`;
}

export function usePlaceNavigate() {
  const navigate = useNavigate();
  return useCallback(
    (key: PlaceKey) => {
      const place = parsePlaceKey(key);
      if (!place) return;
      if (place.kind === "channel") {
        void navigate({
          to: "/c/$channelId",
          params: { channelId: place.channelId },
        });
      } else if (place.kind === "dm") {
        void navigate({ to: "/dm/$dmId", params: { dmId: place.dmId } });
      } else {
        void navigate({ to: "/group/$dmId", params: { dmId: place.dmId } });
      }
    },
    [navigate],
  );
}
