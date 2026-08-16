// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiMessagingBackend, MessagingAPIError } from "./api-backend";
import { MockMessagingServer } from "./mock-server";
import type {
  Attachment,
  UploadAttachmentInput,
  UploadAttachmentReceipt,
} from "./model";
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
  readonly sent: { content: string; attachments: string[] }[] = [];

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
    this.sent.push({ content: input.content, attachments: input.attachments });
    return super.sendMessage(input);
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

    let drafts = useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY];
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
    drafts = useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY];
    expect(drafts.map((entry) => entry.status)).toEqual(["ready", "ready"]);

    useMessaging.getState().send("with files", "normal");
    expect(server.sent).toEqual([
      { content: "with files", attachments: ["att-1", "att-2"] },
    ]);
    expect(
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY],
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
    expect(server.sent).toEqual([{ content: "", attachments: ["att-only"] }]);
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
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY][0]
        ?.clientNonce;
    server.pendingUploads[0]?.deferred.reject(
      new MessagingAPIError("attachment_quota_exceeded", 507),
    );
    await settle();
    expect(
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY][0],
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
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY][0]?.status,
    ).toBe("ready");

    useMessaging.getState().removeDraftAttachment(nonce ?? "");
    expect(
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY],
    ).toEqual([]);
    useMessaging.getState().send("text", "normal");
    expect(server.sent).toEqual([{ content: "text", attachments: [] }]);
  });

  it("rejects oversized and empty files locally and caps the draft count", () => {
    const server = new UploadControlledServer();
    bootstrapStore(server);
    const big = new File([new Uint8Array(1)], "big.bin");
    Object.defineProperty(big, "size", { value: 20 * 1024 * 1024 + 1 });
    const empty = new File([], "empty.txt");
    useMessaging.getState().addDraftAttachments([big, empty]);
    const drafts = useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY];
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
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY],
    ).toHaveLength(10);
  });

  it("aborts and forgets drafts when the session or scope changes, ignoring late receipts", async () => {
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
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});

    // A receipt arriving for the old session cannot resurrect a draft.
    upload?.deferred.resolve(receipt("att-late", "late.txt"));
    await settle();
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
  });

  it("scope switches clear drafts too", () => {
    bindMessagingSessionIdentity("human-self");
    bindMessagingScope(MESSAGING_SCOPE);
    useMessaging.setState({
      ready: true,
      self: { kind: "human", humanId: "self" },
      selfKey: "human:self",
      activePlaceKey: CHANNEL_KEY,
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Promise<Response>(() => {})),
    );
    useMessaging
      .getState()
      .addDraftAttachments([
        new File(["x"], "scoped.txt", { type: "text/plain" }),
      ]);
    expect(
      useMessaging.getState().draftAttachmentsByPlace[CHANNEL_KEY],
    ).toHaveLength(1);
    bindMessagingScope({ ...MESSAGING_SCOPE, authorityEpoch: "2" });
    expect(useMessaging.getState().draftAttachmentsByPlace).toEqual({});
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
