import { MessageSquarePlus } from "lucide-react";
import { useAuth } from "../auth/auth-context";
import {
  participantInstallation,
  useParticipantApps,
} from "../participant/app-store";
import { FeedbackInbox, type InboxLocation } from "./inbox";

export function FeedbackGate({
  location,
  navigate,
}: {
  location: InboxLocation;
  navigate(location: InboxLocation): void;
}) {
  const { authenticated, user } = useAuth();
  const apps = useParticipantApps();
  const exactOwner =
    authenticated &&
    user &&
    apps.owner?.kind === "participant" &&
    apps.owner.participant.kind === "human" &&
    apps.owner.participant.humanId === user.id;
  const installation = participantInstallation(apps.installations, "feedback");
  const descriptor = apps.catalog.find(
    (app) => app.appId === "feedback" && app.participantOwnerAllowed,
  );
  if (
    exactOwner &&
    installation !== "duplicate" &&
    installation?.state === "enabled"
  )
    return (
      <FeedbackInbox
        key={user.id}
        actor={`human:${user.id}`}
        location={location}
        navigate={navigate}
      />
    );
  const loading =
    !exactOwner || apps.status === "idle" || apps.status === "loading";
  const title = loading
    ? "Feedbackを確認しています…"
    : apps.status === "error"
      ? "Feedbackを読み込めませんでした"
      : installation === "duplicate"
        ? "Feedbackの導入状態を確認してください"
        : installation
          ? "Feedbackは無効になっています"
          : "気づいたことを、会話に。";
  const run = (action: () => Promise<unknown>) =>
    void action().catch(() => undefined);
  return (
    <main className="feedback-app">
      <section className="feedback-welcome" aria-live="polite">
        <span className="feedback-welcome-icon">
          <MessageSquarePlus size={26} />
        </span>
        <h1>{title}</h1>
        {!loading &&
          apps.status !== "error" &&
          installation !== "duplicate" && (
            <p>
              Sumiで気づいたことや相談を、開発側と話せる場所です。
              <br />
              Workspaceを切り替えても、会話の続きへ戻れます。
            </p>
          )}
        {!loading && apps.status === "error" ? (
          <button
            type="button"
            className="feedback-primary"
            onClick={() => run(apps.refresh)}
          >
            再試行
          </button>
        ) : !loading && descriptor && installation !== "duplicate" ? (
          <button
            type="button"
            className="feedback-primary"
            disabled={apps.mutation !== null}
            onClick={() =>
              run(() =>
                installation
                  ? apps.setInstallationState(
                      installation.installationId,
                      "enabled",
                    )
                  : apps.installApp("feedback"),
              )
            }
          >
            {apps.mutation
              ? "準備中…"
              : installation
                ? "有効にする"
                : "Feedbackを導入"}
          </button>
        ) : null}
        {!loading && !descriptor && apps.status !== "error" && (
          <p>この環境ではまだ導入できません。</p>
        )}
        {apps.errorCode && apps.status !== "error" && (
          <p role="alert">導入できませんでした。もう一度お試しください。</p>
        )}
      </section>
    </main>
  );
}
