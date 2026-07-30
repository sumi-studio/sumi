import { createFileRoute } from "@tanstack/react-router";
import { AuthGate } from "../auth/auth-gate";
import { DirectChatGate } from "../auth/direct-chat-gate";

export const Route = createFileRoute("/")({
  component: () => (
    <AuthGate>
      <DirectChatGate />
    </AuthGate>
  ),
});
