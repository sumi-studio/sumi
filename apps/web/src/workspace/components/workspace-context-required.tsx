import { Button } from "@sumi/ui/components/button";
import { useNavigate } from "@tanstack/react-router";
import { Building2 } from "lucide-react";
import { AuthGate } from "../../auth/auth-gate";

/** Legacy global place URLs fail closed instead of guessing a Workspace. */
export function WorkspaceContextRequired() {
  const navigate = useNavigate();
  return (
    <AuthGate>
      <main className="grid min-h-dvh place-items-center bg-background px-6 text-foreground">
        <section className="flex max-w-md flex-col items-center text-center">
          <span className="mb-5 grid size-12 place-items-center rounded-xl border border-border bg-muted/20">
            <Building2 className="size-5" />
          </span>
          <h1 className="font-semibold text-xl">Workspaceを選んでください</h1>
          <p className="mt-2 text-muted-foreground text-sm leading-6">
            このリンクだけでは、どのWorkspaceを開くか分かりません。Workspace一覧から選んでください。
          </p>
          <Button className="mt-5" onClick={() => void navigate({ to: "/" })}>
            Workspace一覧へ
          </Button>
        </section>
      </main>
    </AuthGate>
  );
}
