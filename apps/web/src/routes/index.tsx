import { createFileRoute } from "@tanstack/react-router";
import { ChatScreen } from "../components/chat-screen";

export const Route = createFileRoute("/")({ component: ChatScreen });
