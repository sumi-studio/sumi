import { useAuth } from "./auth-context";
import { AuthOutcomeNotice } from "./auth-outcome-notice";

/**
 * A single, app-lifetime announcement surface for terminal auth outcomes.
 * AuthProvider owns the scoped receipt state; this host only presents it.
 */
export function AuthOutcomeNoticeHost() {
  const {
    canUseDirectChat,
    dismissOutcomeNotice,
    emailLinkCallbackPending,
    outcomeNotice,
  } = useAuth();
  const visibleNotice =
    canUseDirectChat && !emailLinkCallbackPending ? outcomeNotice : null;

  return (
    <div role="status" aria-live="polite" aria-atomic="true">
      {visibleNotice && (
        <AuthOutcomeNotice
          notice={visibleNotice}
          onDismiss={dismissOutcomeNotice}
        />
      )}
    </div>
  );
}
