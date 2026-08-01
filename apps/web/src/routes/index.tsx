import { createFileRoute, useNavigate } from "@tanstack/react-router";
import { useEffect } from "react";
import { useMessaging } from "../messaging/store";

/**
 * ルートはホームの入口。最初のchannelへリダイレクトし、以降の現在地は
 * URL（/c/:id、/dm/:id）が正本になる。
 */
export const Route = createFileRoute("/")({
  component: HomeRedirect,
});

function HomeRedirect() {
  const init = useMessaging((state) => state.init);
  const ready = useMessaging((state) => state.ready);
  const channels = useMessaging((state) => state.channels);
  const navigate = useNavigate();

  useEffect(() => {
    init();
  }, [init]);

  useEffect(() => {
    if (!ready) return;
    const first = channels[0];
    if (first) {
      void navigate({
        to: "/c/$channelId",
        params: { channelId: first.channelId },
        replace: true,
      });
    }
  }, [ready, channels, navigate]);

  return (
    <div className="flex h-dvh items-center justify-center bg-background text-muted-foreground text-sm">
      読み込み中…
    </div>
  );
}
