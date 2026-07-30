import { Button } from "@sumi/ui/components/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@sumi/ui/components/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@sumi/ui/components/tooltip";
import {
  Check,
  ChevronRight,
  MessageCircle,
  Monitor,
  Moon,
  Palette,
  Settings,
  Sun,
} from "lucide-react";
import type { ComponentType, ReactElement } from "react";
import { type ThemePreference, useTheme } from "../theme/theme-provider";

const THEME_OPTIONS: Array<{
  id: ThemePreference;
  label: string;
  icon: ComponentType<{ className?: string }>;
}> = [
  { id: "system", label: "システム", icon: Monitor },
  { id: "light", label: "ライト", icon: Sun },
  { id: "dark", label: "ダーク", icon: Moon },
];

/**
 * Only the working Talk surface is exposed. Future shared-workspace apps should
 * appear here when they have real product routes, not as local demo state.
 */
export function AppNavigation() {
  return (
    <aside className="app-sidebar flex h-dvh w-12 shrink-0 flex-col overflow-clip">
      <nav className="flex flex-col gap-1 px-1 py-2" aria-label="Sumi">
        <NavigationTooltip label="トーク">
          <Button
            variant="ghost"
            size="icon"
            aria-label="トーク"
            aria-current="page"
            className="size-10 bg-interactive-active"
          >
            <MessageCircle className="size-4" />
          </Button>
        </NavigationTooltip>
      </nav>
      <div className="mt-auto px-2 pb-3">
        <SettingsPopover />
      </div>
    </aside>
  );
}

function SettingsPopover() {
  return (
    <Popover>
      <Tooltip>
        <TooltipTrigger
          render={
            <PopoverTrigger
              render={
                <Button
                  variant="ghost"
                  size="icon"
                  aria-label="設定"
                  className="size-8 shrink-0"
                />
              }
            />
          }
        >
          <Settings className="size-4" />
        </TooltipTrigger>
        <TooltipContent side="right">設定</TooltipContent>
      </Tooltip>
      <PopoverContent side="top" align="start" aria-label="設定">
        <ThemePicker />
      </PopoverContent>
    </Popover>
  );
}

function NavigationTooltip({
  label,
  children,
}: {
  label: string;
  children: ReactElement;
}) {
  return (
    <Tooltip>
      <TooltipTrigger render={children} />
      <TooltipContent side="right">{label}</TooltipContent>
    </Tooltip>
  );
}

function ThemePicker() {
  const { theme, setTheme } = useTheme();
  return (
    <Popover>
      <PopoverTrigger
        openOnHover
        delay={0}
        closeDelay={120}
        render={
          <Button
            variant="ghost"
            className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
          />
        }
      >
        <Palette className="size-4" />
        <span>テーマ</span>
        <ChevronRight className="ml-auto size-4 text-neutral-400" />
      </PopoverTrigger>
      <PopoverContent
        side="right"
        align="start"
        sideOffset={4}
        aria-label="テーマ"
      >
        {THEME_OPTIONS.map(({ id, label, icon: Icon }) => (
          <Button
            key={id}
            variant="ghost"
            onClick={() => setTheme(id)}
            className="w-full justify-start gap-2 px-2.5 text-popover-foreground hover:text-popover-foreground"
          >
            <Icon className="size-4" />
            {label}
            {theme === id && <Check className="ml-auto size-4" />}
          </Button>
        ))}
      </PopoverContent>
    </Popover>
  );
}
