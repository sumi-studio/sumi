import { useNavigate } from "@tanstack/react-router";
import { useCallback } from "react";
import { type PlaceKey, parsePlaceKey } from "./model";
import { getMessagingScope } from "./store";

/**
 * placeはWorkspaceを含むURLを持つ。リロード・戻る/進む・リンク共有を
 * 成立させつつ、同じplace IDを別Workspaceのauthorityで開かない。
 */
export function placePath(workspaceId: string, key: PlaceKey): string {
  const place = parsePlaceKey(key);
  if (!place) return "/";
  const base = `/w/${encodeURIComponent(workspaceId)}/messaging`;
  if (place.kind === "channel") return `${base}/c/${place.channelId}`;
  if (place.kind === "thread") return `${base}/t/${place.threadId}`;
  if (place.kind === "dm") return `${base}/dm/${place.dmId}`;
  return `${base}/group/${place.dmId}`;
}

export function usePlaceNavigate() {
  const navigate = useNavigate();
  return useCallback(
    (key: PlaceKey) => {
      const place = parsePlaceKey(key);
      const workspaceId = getMessagingScope()?.workspaceId;
      if (!place || !workspaceId) return;
      if (place.kind === "channel") {
        void navigate({
          to: "/w/$workspaceId/messaging/c/$channelId",
          params: { workspaceId, channelId: place.channelId },
        });
      } else if (place.kind === "thread") {
        void navigate({
          to: "/w/$workspaceId/messaging/t/$threadId",
          params: { workspaceId, threadId: place.threadId },
        });
      } else if (place.kind === "dm") {
        void navigate({
          to: "/w/$workspaceId/messaging/dm/$dmId",
          params: { workspaceId, dmId: place.dmId },
        });
      } else {
        void navigate({
          to: "/w/$workspaceId/messaging/group/$dmId",
          params: { workspaceId, dmId: place.dmId },
        });
      }
    },
    [navigate],
  );
}
