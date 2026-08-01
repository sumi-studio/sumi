import { useEffect, useRef } from "react";
import { useConversation } from "../agent/store";
import { ChatScreen } from "../components/chat-screen";
import { useAuth } from "./auth-context";

/**
 * A failed authenticated upgrade is indistinguishable from a transient close
 * at the WebSocket layer. Re-check the HttpOnly session after a live
 * connection attempt closes; AuthGate then unmounts ChatScreen and cancels its
 * reconnect timer when the session is no longer usable.
 */
export function DirectChatGate() {
  const connection = useConversation((state) => state.connection);
  const previousConnection = useRef(connection);
  const { refreshSession } = useAuth();

  useEffect(() => {
    const previous = previousConnection.current;
    previousConnection.current = connection;
    if (
      connection === "closed" &&
      (previous === "connecting" || previous === "connected")
    ) {
      void refreshSession();
    }
  }, [connection, refreshSession]);

  return <ChatScreen />;
}
