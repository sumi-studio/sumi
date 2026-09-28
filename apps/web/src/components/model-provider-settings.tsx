import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@sumi/ui/components/sheet";
import type { APIConnectionsClient } from "../lib/api-connections";
import type { UsageAPI } from "../lib/usage";
import { APIConnectionSettings } from "./api-connection-settings";
import { UsageSettings } from "./usage-settings";

export function ModelProviderSettings({
  open,
  onOpenChange,
  connectionsAPI,
  usageAPI,
}: {
  open: boolean;
  onOpenChange(open: boolean): void;
  connectionsAPI?: APIConnectionsClient;
  usageAPI?: UsageAPI;
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="data-[side=right]:w-full sm:data-[side=right]:w-[28rem]">
        <SheetHeader className="border-border border-b px-6 py-5 pr-12">
          <SheetTitle>AIの接続</SheetTitle>
          <SheetDescription>
            APIキー、またはサーバーが対応していればChatGPTのサブスクリプションで、AIサービスをSumiに接続します。
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-6">
          {open && <APIConnectionSettings client={connectionsAPI} />}
          {open && <UsageSettings client={usageAPI} />}
        </div>
      </SheetContent>
    </Sheet>
  );
}
