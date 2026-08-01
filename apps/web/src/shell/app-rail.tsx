import { Button } from "@sumi/ui/components/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@sumi/ui/components/tooltip";
import { useNavigate } from "@tanstack/react-router";
import { LOCAL_APP_DESCRIPTORS } from "./app-descriptors";

/**
 * 左端のアプリレール。Discordのサーバー列に相当する場所で、Sumiでは
 * マイクロアプリ（ホーム=メッセージング、将来のMail/Calendar…）が並ぶ。
 * 見た目の文法は既存のAppNavigation（direct chat側）と同一。
 */
export function AppRail({ activeAppId }: { activeAppId: string }) {
  const navigate = useNavigate();
  return (
    <aside className="app-sidebar flex h-dvh w-12 shrink-0 flex-col overflow-clip">
      <nav className="flex flex-col gap-1 px-1 py-2" aria-label="Sumi">
        {LOCAL_APP_DESCRIPTORS.map((app) => {
          const Icon = app.icon;
          const active = app.id === activeAppId;
          return (
            <Tooltip key={app.id}>
              <TooltipTrigger
                render={
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={app.label}
                    aria-current={active ? "page" : undefined}
                    onClick={() => void navigate({ to: app.route })}
                    className={`size-10 ${active ? "bg-interactive-active" : ""}`}
                  />
                }
              >
                <Icon className="size-4" />
              </TooltipTrigger>
              <TooltipContent side="right">{app.label}</TooltipContent>
            </Tooltip>
          );
        })}
      </nav>
    </aside>
  );
}
