import { cn } from "@sumi/ui";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
  component: Home,
});

function Home() {
  return (
    <main className={cn("flex min-h-dvh items-center justify-center")}>
      <h1 className="font-semibold text-2xl">Sumi</h1>
    </main>
  );
}
