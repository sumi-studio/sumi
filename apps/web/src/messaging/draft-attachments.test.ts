// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend, MessagingAPIError } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type {
  Attachment,
  AttachmentDraftPatch,
  UploadAttachmentInput,
  UploadAttachmentReceipt,
} from "./model";
import { participantKey } from "./model";
import type { MessagingScope } from "./scope";
import {
  expectScopedMessagingPath,
  MESSAGING_SCOPE,
} from "./scope.test-support";
import {
  bindMessagingScope,
  bindMessagingSessionIdentity,
  installMessagingBackend,
  useMessaging,
} from "./store";

const CHANNEL_KEY = "channel:ch-general";
const CHANNEL = { kind: "channel", channelId: "ch-general" } as const;

class Deferred<T> {
  resolve!: (value: T) => void;
  reject!: (error: unknown) => void;
  readonly promise = new Promise<T>((resolve, reject) => {
    this.resolve = resolve;
    this.reject = reject;
  });
}

/** Mock server whose uploads settle only when the test says so. */
class UploadControlledServer extends MockMessagingServer {
  readonly pendingUploads: {
    input: UploadAttachmentInput;
    deferred: Deferred<UploadAttachmentReceipt>;
  }[] = [];
  readonly sent: Parameters<MockMessagingServer["sendMessage"]>[0][] = [];
  readonly pendingEdits: {
    attachmentId: string;
    patch: AttachmentDraftPatch;
    deferred: Deferred<Attachment>;
  }[] = [];

  override uploadAttachment(
    input: UploadAttachmentInput,
  ): Promise<UploadAttachmentReceipt> {
    const deferred = new Deferred<UploadAttachmentReceipt>();
    this.pendingUploads.push({ input, deferred });
    return deferred.promise;
  }

  override sendMessage(
    input: Parameters<MockMessagingServer["sendMessage"]>[0],
  ) {
    this.sent.push(input);
    return super.sendMessage(input);
  }

  override updateDraftAttachment(
    attachmentId: string,
    patch: AttachmentDraftPatch,
  ): Promise<Attachment> {
    const deferred = new Deferred<Attachment>();
    this.pendingEdits.push({ attachmentId, patch, deferred });
    return deferred.promise;
  }
}

function receipt(
  id: string,
  filename: string,
  mime = "text/plain",
): UploadAttachmentReceipt {
  const attachment: Attachment = {
    attachmentId: id,
    filename,
    mime,
    sizeBytes: 3,
    sha256: "",
    position: 0,
    spoiler: false,
    alt: "",
  };
  return { attachment, created: true };
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("composer draft attachments", () => {
  afterEach(() => {
    bindMessagingSessionIdentity(null);
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  function bootstrapStore(server: UploadControlledServer): void {
    bindMessagingSessionIdentity("human-self");
    installMessagingBackend(server);
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "self" },
      selfKey: "human:self",
      activePlaceKey: CHANNEL_KEY,
      messagesByPlace: { [CHANNEL_KEY]: [] },
    });
  }

  it("uploads picked files per stable nonce, sends the ids in order, and clears the drafts", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    const first = new File(["aaa"], "one.txt", { type: "text/plain" });
    const second = new File(["bbb"], "two.png", { type: "image/png" });
    useMessaging.getState().addDraftAttachments([first, second]);

    let drafts = useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments;
    expect(drafts.map((entry) => entry.status)).toEqual([
      "uploading",
      "uploading",
    ]);
    expect(server.pendingUploads).toHaveLength(2);
    expect(server.pendingUploads[0]?.input).toMatchObject({
      place: CHANNEL,
      filename: "one.txt",
      contentType: "text/plain",
      body: first,
    });
    expect(server.pendingUploads[0]?.input.clientNonce).toBe(
      drafts[0]?.clientNonce,
    );

    // Nothing may be sent while uploads are outstanding.
    useMessaging.getState().send("with files", "normal");
    expect(server.sent).toHaveLength(0);

    // The second file finishes first; the sender's order still wins.
    server.pendingUploads[1]?.deferred.resolve(
      receipt("att-2", "two.png", "image/png"),
    );
    await settle();
    useMessaging.getState().send("with files", "normal");
    expect(server.sent).toHaveLength(0);
    server.pendingUploads[0]?.deferred.resolve(receipt("att-1", "one.txt"));
    await settle();
    drafts = useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments;
    expect(drafts.map((entry) => entry.status)).toEqual(["ready", "ready"]);

    useMessaging.getState().send("with files", "normal");
    expect(server.sent).toMatchObject([
      { content: "with files", attachments: ["att-1", "att-2"] },
    ]);
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments,
    ).toEqual([]);
    const pending = useMessaging.getState().pendingByPlace[CHANNEL_KEY];
    expect(
      pending?.[0]?.attachments.map((entry) => entry.attachmentId),
    ).toEqual(["att-1", "att-2"]);
  });

  it("sends attachment-only messages and refuses empty ones", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    useMessaging.getState().send("   ", "normal");
    expect(server.sent).toHaveLength(0);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "only.txt", { type: "text/plain" }),
      ]);
    server.pendingUploads[0]?.deferred.resolve(receipt("att-only", "only.txt"));
    await settle();
    useMessaging.getState().send("", "normal");
    expect(server.sent).toMatchObject([
      { content: "", attachments: ["att-only"] },
    ]);
  });

  it("shows the server-canonical filename after upload", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "a-very-long-local-name.txt", { type: "text/plain" }),
      ]);
    server.pendingUploads[0]?.deferred.resolve(receipt("att-name", "name.txt"));
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0],
    ).toMatchObject({ filename: "name.txt", status: "ready" });
  });

  it("marks failed uploads, retries with the same nonce, and drops removed drafts", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "flaky.txt", { type: "text/plain" }),
      ]);
    const nonce =
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0]
        ?.clientNonce;
    server.pendingUploads[0]?.deferred.reject(
      new MessagingAPIError("attachment_quota_exceeded", 507),
    );
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0],
    ).toMatchObject({
      status: "failed",
      errorCode: "attachment_quota_exceeded",
    });
    useMessaging.getState().send("text", "normal");
    expect(server.sent).toHaveLength(0);

    useMessaging.getState().retryDraftAttachment(nonce ?? "");
    expect(server.pendingUploads).toHaveLength(2);
    expect(server.pendingUploads[1]?.input.clientNonce).toBe(nonce);
    server.pendingUploads[1]?.deferred.resolve(
      receipt("att-retry", "flaky.txt"),
    );
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0]?.status,
    ).toBe("ready");

    useMessaging.getState().removeDraftAttachment(nonce ?? "");
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments,
    ).toEqual([]);
    useMessaging.getState().send("text", "normal");
    expect(server.sent).toMatchObject([{ content: "text", attachments: [] }]);
  });

  it("keeps a rejected attachment edit visible and unsendable until its PATCH succeeds", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "before.txt", { type: "text/plain" }),
      ]);
    server.pendingUploads[0]?.deferred.resolve(
      receipt("att-edit", "before.txt"),
    );
    await settle();
    const nonce =
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0]
        ?.clientNonce ?? "";
    const patch = {
      filename: "after.txt",
      alt: "保存される説明",
      spoiler: true,
    };

    useMessaging.getState().editDraftAttachment(nonce, patch);
    expect(server.pendingEdits).toHaveLength(1);
    server.pendingEdits[0]?.deferred.reject(
      new MessagingAPIError("invalid_request", 400),
    );
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0],
    ).toMatchObject({
      status: "edit_failed",
      errorCode: "invalid_request",
      editPatch: patch,
    });
    useMessaging.getState().send("must stay in the composer", "normal");
    expect(server.sent).toHaveLength(0);

    useMessaging.getState().retryDraftAttachment(nonce);
    expect(server.pendingEdits).toHaveLength(2);
    expect(server.pendingEdits[1]).toMatchObject({
      attachmentId: "att-edit",
      patch,
    });
    server.pendingEdits[1]?.deferred.resolve({
      ...receipt("att-edit", "after.txt").attachment,
      alt: "保存される説明",
      spoiler: true,
    });
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0],
    ).toMatchObject({
      status: "ready",
      filename: "after.txt",
      errorCode: undefined,
      editPatch: undefined,
      attachment: {
        attachmentId: "att-edit",
        alt: "保存される説明",
        spoiler: true,
      },
    });
    useMessaging.getState().send("saved", "normal");
    expect(server.sent).toMatchObject([
      { content: "saved", attachments: ["att-edit"] },
    ]);
  });

  it("rejects oversized and empty files locally and caps the draft count", () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    const big = new File([new Uint8Array(1)], "big.bin");
    Object.defineProperty(big, "size", { value: 20 * 1024 * 1024 + 1 });
    const empty = new File([], "empty.txt");
    useMessaging.getState().addDraftAttachments([big, empty]);
    const drafts =
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments;
    expect(drafts.map((entry) => entry.errorCode)).toEqual([
      "attachment_too_large",
      "attachment_empty",
    ]);
    expect(server.pendingUploads).toHaveLength(0);
    useMessaging
      .getState()
      .addDraftAttachments(
        Array.from(
          { length: 12 },
          (_, index) => new File(["x"], `f${index}.txt`),
        ),
      );
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments,
    ).toHaveLength(10);
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachmentOverflow,
    ).toBe(4);
  });

  it("aborts and forgets drafts when the signed-in Human changes, ignoring late receipts", async () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "late.txt", { type: "text/plain" }),
      ]);
    const upload = server.pendingUploads[0];
    expect(upload?.input.signal?.aborted).toBe(false);

    bindMessagingSessionIdentity("someone-else");
    expect(upload?.input.signal?.aborted).toBe(true);
    expect(useMessaging.getState().draftByPlace).toEqual({});

    // A receipt arriving for the old session cannot resurrect a draft.
    upload?.deferred.resolve(receipt("att-late", "late.txt"));
    await settle();
    expect(useMessaging.getState().draftByPlace).toEqual({});
  });

  async function enterWorkspace(
    server: UploadControlledServer,
    scope: MessagingScope = MESSAGING_SCOPE,
  ) {
    bindMessagingScope(scope);
    installMessagingBackend(server);
    const snapshot = await server.bootstrap();
    useMessaging.setState({
      ready: true,
      self: snapshot.self,
      selfKey: "human:self",
      channels: snapshot.channels,
      membersByKey: Object.fromEntries(
        snapshot.members.map((member) => [
          participantKey(member.participant),
          member,
        ]),
      ),
    });
    useMessaging.getState().selectPlace(CHANNEL_KEY);
  }

  it("restores each conversation's text and reply target and sends to the selected conversation", async () => {
    bindMessagingSessionIdentity("human-self");
    const server = new UploadControlledServer();
    await enterWorkspace(server);
    const other = {
      ...useMessaging.getState().channels[0],
      channelId: "other",
      name: "other",
    };
    useMessaging.setState((state) => ({
      channels: [...state.channels, other],
    }));
    useMessaging.getState().setDraft(CHANNEL_KEY, "日本語の返信を書きかけです");
    useMessaging.getState().setReplyTarget("reply-in-general");
    useMessaging.getState().selectPlace("channel:other");
    useMessaging.getState().setDraft("channel:other", "別の相談");
    useMessaging.getState().setReplyTarget("reply-in-other");
    useMessaging.getState().selectPlace(CHANNEL_KEY);
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY].text).toBe(
      "日本語の返信を書きかけです",
    );
    expect(
      useMessaging
        .getState()
        .send(useMessaging.getState().draftByPlace[CHANNEL_KEY].text, "normal"),
    ).toBe(true);
    expect(server.sent[0]).toMatchObject({
      place: CHANNEL,
      content: "日本語の返信を書きかけです",
      replyTo: "reply-in-general",
    });
    useMessaging.getState().selectPlace("channel:other");
    expect(useMessaging.getState().draftByPlace["channel:other"]).toMatchObject(
      { text: "別の相談", replyTarget: { messageId: "reply-in-other" } },
    );
  });

  it("parks workspace drafts through the picker, resumes original Files, and fences late uploads even after returning", async () => {
    const createPreview = vi.fn(() => "blob:draft-preview");
    const revokePreview = vi.fn();
    vi.stubGlobal(
      "URL",
      class extends URL {
        static createObjectURL = createPreview;
        static revokeObjectURL = revokePreview;
      },
    );
    bindMessagingSessionIdentity("human-self");
    const first = new UploadControlledServer();
    await enterWorkspace(first);
    const file = new File(["image"], "案内.png", { type: "image/png" });
    useMessaging.getState().setDraft(CHANNEL_KEY, "展示のご案内を書きかけです");
    useMessaging.getState().setReplyTarget("invitation-question");
    useMessaging
      .getState()
      .setDraftSelection(
        CHANNEL_KEY,
        { start: 2, end: 5, direction: "backward", scrollTop: 70 },
        useMessaging.getState().transportGeneration,
      );
    useMessaging
      .getState()
      .addDraftAttachments([
        file,
        new File(["doc"], "会期.txt", { type: "text/plain" }),
      ]);
    const oldUpload = first.pendingUploads[0];
    first.pendingUploads[1].deferred.resolve(receipt("att-ready", "会期.txt"));
    await settle();
    const oldGeneration = useMessaging.getState().transportGeneration;
    bindMessagingScope(null); // The actual Workspace picker unbinds first.
    expect(oldUpload.input.signal?.aborted).toBe(true);
    expect(useMessaging.getState().draftByPlace).toEqual({});
    expect(revokePreview).not.toHaveBeenCalled();
    const otherScope = {
      ...MESSAGING_SCOPE,
      workspaceId: "workspace-other",
      installationId: "installation-other",
    };
    const second = new UploadControlledServer();
    await enterWorkspace(second, otherScope);
    useMessaging.getState().setDraft(CHANNEL_KEY, "もう一つのWorkspaceの草稿");
    // A late component callback is as stale as its upload, even with the same place id.
    useMessaging.getState().setDraft(CHANNEL_KEY, "stale text", oldGeneration);
    useMessaging
      .getState()
      .setDraftSelection(
        CHANNEL_KEY,
        { start: 0, end: 0, direction: "none", scrollTop: 0 },
        oldGeneration,
      );
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY].text).toBe(
      "もう一つのWorkspaceの草稿",
    );
    expect(second.pendingUploads).toHaveLength(0);
    bindMessagingScope(null);
    const returned = new UploadControlledServer();
    await enterWorkspace(returned);
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY]).toMatchObject({
      text: "展示のご案内を書きかけです",
      replyTarget: { messageId: "invitation-question" },
      selection: { start: 2, end: 5, direction: "backward", scrollTop: 70 },
    });
    expect(returned.pendingUploads).toHaveLength(1);
    expect(returned.pendingUploads[0].input).toMatchObject({
      clientNonce: oldUpload.input.clientNonce,
    });
    expect(returned.pendingUploads[0].input.body).toBe(file);
    expect(revokePreview).not.toHaveBeenCalled();
    oldUpload.deferred.resolve(receipt("att-stale", "案内.png", "image/png"));
    await settle();
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[0].status,
    ).toBe("uploading");
    returned.pendingUploads[0].deferred.resolve(
      receipt("att-current", "案内.png", "image/png"),
    );
    await settle();
    // Keep the send unresolved to distinguish submitted work from a new unsent draft.
    vi.spyOn(returned, "sendMessage").mockImplementation((input) => {
      returned.sent.push(input);
      return new Promise(() => {});
    });
    useMessaging
      .getState()
      .send(useMessaging.getState().draftByPlace[CHANNEL_KEY].text, "normal");
    expect(returned.sent[0]).toMatchObject({
      attachments: ["att-current", "att-ready"],
      replyTo: "invitation-question",
    });
    expect(useMessaging.getState().pendingByPlace[CHANNEL_KEY]).toHaveLength(1);
    expect(revokePreview).toHaveBeenCalledWith("blob:draft-preview");
    useMessaging.getState().setDraft(CHANNEL_KEY, "送信後の新しい草稿");
    await enterWorkspace(new UploadControlledServer(), otherScope);
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY].text).toBe(
      "もう一つのWorkspaceの草稿",
    );
    await enterWorkspace(new UploadControlledServer());
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY]).toMatchObject({
      text: "送信後の新しい草稿",
      replyTarget: null,
      attachments: [],
    });
  });

  it("keeps authored drafts across authority epochs but separates a new installation", async () => {
    bindMessagingSessionIdentity("human-self");
    const first = new UploadControlledServer();
    await enterWorkspace(first);
    const file = new File(["x"], "scoped.txt", { type: "text/plain" });
    useMessaging.getState().setDraft(CHANNEL_KEY, "同じ設置先の草稿");
    useMessaging.getState().setReplyTarget("reply-old-epoch");
    useMessaging
      .getState()
      .addDraftAttachments([
        file,
        new File(["ready"], "ready.txt", { type: "text/plain" }),
      ]);
    first.pendingUploads[1].deferred.resolve(
      receipt("att-previous-epoch", "ready.txt"),
    );
    await settle();
    const next = new UploadControlledServer();
    await enterWorkspace(next, { ...MESSAGING_SCOPE, authorityEpoch: "2" });
    expect(first.pendingUploads[0].input.signal?.aborted).toBe(true);
    expect(next.pendingUploads).toHaveLength(1);
    expect(next.pendingUploads[0].input.body).toBe(file);
    expect(
      useMessaging.getState().draftByPlace[CHANNEL_KEY].attachments[1],
    ).toMatchObject({
      status: "ready",
      attachment: { attachmentId: "att-previous-epoch" },
    });
    expect(useMessaging.getState().draftByPlace[CHANNEL_KEY]).toMatchObject({
      text: "同じ設置先の草稿",
      replyTarget: { messageId: "reply-old-epoch" },
    });
    await enterWorkspace(new UploadControlledServer(), {
      ...MESSAGING_SCOPE,
      installationId: "new-installation",
    });
    expect(useMessaging.getState().draftByPlace).toEqual({});
  });

  it.each([
    "logout",
    "human-switch",
  ])("discards both parked and active drafts on %s", async (change) => {
    const revoke = vi.fn();
    vi.stubGlobal(
      "URL",
      Object.assign(URL, {
        createObjectURL: () => "blob:private",
        revokeObjectURL: revoke,
      }),
    );
    bindMessagingSessionIdentity("human-self");
    const first = new UploadControlledServer();
    await enterWorkspace(first);
    useMessaging.getState().setDraft(CHANNEL_KEY, "private A");
    useMessaging.getState().setReplyTarget("private-parent");
    useMessaging
      .getState()
      .addDraftAttachments([new File(["a"], "a.png", { type: "image/png" })]);
    const second = new UploadControlledServer();
    await enterWorkspace(second, { ...MESSAGING_SCOPE, workspaceId: "other" });
    useMessaging.getState().setDraft(CHANNEL_KEY, "private B");
    useMessaging
      .getState()
      .addDraftAttachments([new File(["b"], "b.png", { type: "image/png" })]);
    bindMessagingSessionIdentity(change === "logout" ? null : "human-other");
    expect(revoke).toHaveBeenCalledTimes(2);
    expect(second.pendingUploads[0].input.signal?.aborted).toBe(true);
    first.pendingUploads[0].deferred.resolve(
      receipt("private-late", "a.png", "image/png"),
    );
    await settle();
    expect(useMessaging.getState().draftByPlace).toEqual({});
    bindMessagingSessionIdentity("human-self");
    const fresh = new UploadControlledServer();
    await enterWorkspace(fresh);
    expect(useMessaging.getState().draftByPlace).toEqual({});
    expect(fresh.pendingUploads).toHaveLength(0);
  });
});

describe("ApiMessagingBackend attachments", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("uploads raw bytes with the nonce and filename headers under the exact scope", async () => {
    const fetchMock = vi.fn(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const path = expectScopedMessagingPath(input);
        expect(path).toBe("/messaging/places/ch-general/attachments");
        expect(init?.method).toBe("POST");
        const headers = new Headers(init?.headers);
        expect(headers.get("Idempotency-Key")).toBe("nonce-file");
        expect(headers.get("X-Sumi-Attachment-Filename")).toBe(
          encodeURIComponent("写真 1.png"),
        );
        expect(headers.get("Content-Type")).toBe("image/png");
        expect(init?.body).toBeInstanceOf(Blob);
        return new Response(
          JSON.stringify({
            attachment: {
              attachment_id: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
              filename: "写真 1.png",
              mime: "image/png",
              size_bytes: 3,
              sha256: "ab",
              position: 0,
              spoiler: false,
              alt: "",
            },
            created: true,
          }),
          { status: 201, headers: { "Content-Type": "application/json" } },
        );
      },
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    const uploaded = await backend.uploadAttachment({
      place: CHANNEL,
      clientNonce: "nonce-file",
      filename: "写真 1.png",
      contentType: "image/png",
      body: new Blob(["png"]),
    });
    expect(uploaded).toEqual({
      attachment: {
        attachmentId: "0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa",
        filename: "写真 1.png",
        mime: "image/png",
        sizeBytes: 3,
        sha256: "ab",
        position: 0,
        spoiler: false,
        alt: "",
      },
      created: true,
    });
    expect(
      expectScopedMessagingPath(
        backend.attachmentURL("0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa"),
      ),
    ).toBe("/messaging/attachments/0190aaaa-aaaa-7aaa-8aaa-aaaaaaaaaaaa");
  });

  it("surfaces the server error code and refuses oversized bodies before fetch", async () => {
    const fetchMock = vi.fn(
      async () =>
        new Response(JSON.stringify({ error: "attachment_quota_exceeded" }), {
          status: 507,
          headers: { "Content-Type": "application/json" },
        }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const backend = new ApiMessagingBackend(MESSAGING_SCOPE);
    await expect(
      backend.uploadAttachment({
        place: CHANNEL,
        clientNonce: "n",
        filename: "f",
        contentType: "",
        body: new Blob(["x"]),
      }),
    ).rejects.toMatchObject({ code: "attachment_quota_exceeded", status: 507 });
    const big = new Blob(["x"]);
    Object.defineProperty(big, "size", { value: 20 * 1024 * 1024 + 1 });
    await expect(
      backend.uploadAttachment({
        place: CHANNEL,
        clientNonce: "n",
        filename: "f",
        contentType: "",
        body: big,
      }),
    ).rejects.toMatchObject({ code: "attachment_too_large" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
