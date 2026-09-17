import type { ToolSpec } from "./provider.ts";

/**
 * Tool registry. The core hands `specs` to the model and routes each tool
 * call through the state service operation ledger. For the slice, only
 * state-internal tools are registered: their effects commit atomically
 * inside the claim transaction (no crash window between effect and receipt).
 * `messaging.send` is state-internal too — its effect is delegated to the
 * Messaging domain, which applies the append in the same claim transaction.
 *
 * message.send is outward-facing — speaking into the shared channel as the
 * secretary — so it may only run as an elevated call the human approves
 * (ADR 0013). A normal-route call is recorded as a structured block: the
 * Normal route never prompts the human and is never silently promoted.
 * The model may elevate any call itself via the route field of the
 * provider envelope.
 *
 * Other external-side-effect tools (send email, call a paid API) are
 * deliberately NOT in this registry — they need an external executor before
 * the model may invoke them.
 */

export interface RegisteredTool extends ToolSpec {
  /** Marked internal: effect applied atomically by the state service. */
  readonly internal: true;
  /**
   * Marked delegated: the effect only exists when the host registered a
   * ToolEffect for it (Go Store.RegisterEffect — e.g. messaging.send needs
   * the Messaging domain wired). A bare state service cannot execute it, so
   * it is advertised to the model only when the state lists it claimable.
   */
  readonly delegated?: true;
}

export const INTERNAL_TOOLS: RegisteredTool[] = [
  {
    internal: true,
    name: "schedule.set",
    description:
      "Schedule a future wake-up for yourself. The payload becomes a durable input at wake_at. Use for reminders and follow-ups.",
    parameters: {
      type: "object",
      properties: {
        wake_at: { type: "string", description: "RFC3339 timestamp to wake" },
        payload: {
          type: "object",
          description: "input payload delivered at wake",
        },
      },
      required: ["wake_at"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.send",
    description:
      "Post a message into a shared Messaging place (channel, thread, DM, or group DM) as yourself, so the people and secretaries there see it. Use the place_id shown in the input's marker; pass its message_id as reply_to to answer that message directly. Attach files you uploaded through messaging.upload_attachment by passing their attachment ids. This is for genuinely replying — do not post merely to acknowledge ambient messages.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "the Messaging place id to post into",
        },
        content: {
          type: "string",
          description:
            "the message text; may be empty when attachments are bound",
        },
        reply_to: {
          type: "string",
          description: "optional message_id in the same place to reply to",
        },
        urgency: {
          type: "string",
          enum: ["normal", "urgent", "fyi"],
          description:
            "optional urgency; 'urgent' interrupts members whose settings allow it, 'fyi' never notifies",
        },
        attachments: {
          type: "array",
          items: { type: "string" },
          description:
            "optional attachment ids from messaging.upload_attachment to bind to this message",
        },
      },
      required: ["place_id", "content"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.overview",
    description:
      "Read the shared Messaging workspace as yourself: channels, DMs, threads, members, unread summaries, and read markers — the same overview a human's Messaging screen shows.",
    parameters: {
      type: "object",
      properties: {
        workspace_id: {
          type: "string",
          description: "the workspace id shown in messaging inputs",
        },
      },
      required: ["workspace_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.open",
    description:
      "Open one Messaging place as yourself: its details, members, your read position, and a page of message history (before_seq pages further back). When the page is marked truncated, next_before_seq continues it. Deleted messages appear as tombstones. Works for channels, threads, and DMs you belong to.",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string", description: "the place to open" },
        before_seq: {
          type: "integer",
          description:
            "optional: return messages before this seq to page backward",
        },
        limit: {
          type: "integer",
          description: "optional history page size",
        },
      },
      required: ["place_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.search",
    description:
      "Search messages across the places you can see in a workspace — the same visibility rules as reading them. Returns snippets with message_id, place, and seq; use messaging.open to read the surrounding history. When the result is marked truncated, narrow with place_id or a more specific query.",
    parameters: {
      type: "object",
      properties: {
        query: { type: "string", description: "text to search for" },
        workspace_id: {
          type: "string",
          description: "the workspace to search",
        },
        place_id: {
          type: "string",
          description: "optional: restrict the search to one place",
        },
        limit: { type: "integer", description: "optional result cap" },
      },
      required: ["query", "workspace_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.start_dm",
    description:
      "Open a direct place in a workspace with other members: one other participant creates (or reuses) a DM, two or more create a group DM. You are always one side — name only the others.",
    parameters: {
      type: "object",
      properties: {
        workspace_id: { type: "string", description: "the workspace id" },
        participants: {
          type: "array",
          items: {
            type: "object",
            properties: {
              kind: {
                type: "string",
                enum: ["human", "personality_agent"],
              },
              human_id: { type: "string" },
              personality_agent_id: { type: "string" },
            },
            required: ["kind"],
          },
          description:
            "the other members: {kind:'human', human_id} or {kind:'personality_agent', personality_agent_id}",
        },
      },
      required: ["workspace_id", "participants"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.create_channel",
    description:
      "Create a channel in a workspace you belong to. Retrying a call that already succeeded returns the channel it made instead of a duplicate.",
    parameters: {
      type: "object",
      properties: {
        workspace_id: { type: "string", description: "the workspace id" },
        name: { type: "string", description: "channel name" },
        topic: { type: "string", description: "optional topic" },
        voice: {
          type: "boolean",
          description: "optional: the channel carries calls",
        },
      },
      required: ["workspace_id", "name"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.update_channel",
    description:
      "Rename a channel or change its topic, where you hold channel-editing permission. Pass only the fields to change.",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string", description: "the channel id" },
        name: { type: "string", description: "new name, if changing" },
        topic: { type: "string", description: "new topic, if changing" },
      },
      required: ["place_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.duplicate_channel",
    description:
      "Copy a channel's setup into a new channel in the same workspace, optionally under a new name.",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string", description: "the channel to copy" },
        name: { type: "string", description: "optional new channel name" },
      },
      required: ["place_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.create_thread",
    description:
      "Open a thread in a channel, optionally anchored to a message_id so replies collect under it. If that message already has a thread, you get the existing one.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "the parent channel id",
        },
        name: { type: "string", description: "thread name" },
        message_id: {
          type: "string",
          description: "optional channel message to anchor the thread to",
        },
      },
      required: ["place_id", "name"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.edit_message",
    description:
      "Edit the content of a message you sent. You can only edit your own messages; another author's message is refused. expected_revision is optional — pass it only when you opened the message and want to fail if it changed since.",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string" },
        message_id: { type: "string" },
        content: { type: "string", description: "the replacement text" },
        expected_revision: {
          type: "integer",
          description:
            "optional: the revision you saw; the edit fails if the message moved past it",
        },
      },
      required: ["place_id", "message_id", "content"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.delete_message",
    description:
      "Retract a message. The shared rules apply: your own messages anywhere, and channel moderation where you hold that permission. Deleting leaves a tombstone — the message is not erased from history.",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string" },
        message_id: { type: "string" },
      },
      required: ["place_id", "message_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.notification_settings",
    description:
      "Read or change your own Messaging notification settings. Pass only what changes: defaults_level (all/mentions/mute), per_place overrides, or keywords that should reach you. Omitting a field keeps its stored value; calling with no fields just reads.",
    parameters: {
      type: "object",
      properties: {
        workspace_id: { type: "string", description: "the workspace id" },
        defaults_level: {
          type: "string",
          enum: ["all", "mentions", "mute"],
          description: "your default notification level",
        },
        per_place: {
          type: "array",
          items: {
            type: "object",
            properties: {
              place: {
                type: "object",
                properties: {
                  channel_id: { type: "string" },
                  dm_id: { type: "string" },
                  thread_id: { type: "string" },
                },
              },
              level: { type: "string", enum: ["all", "mentions", "mute"] },
            },
            required: ["place", "level"],
          },
          description:
            "replaces your per-place levels; pass the full list, not a delta",
        },
        keywords: {
          type: "array",
          items: { type: "string" },
          description: "replaces your notification keywords",
        },
      },
      required: ["workspace_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.upload_attachment",
    description:
      "Upload a small file into a place as a draft attachment, then bind it to a message with messaging.send's attachments field. content_base64 is the file's bytes in base64 (at most ~2 MiB — meant for small generated artifacts; larger files go through the human upload lane); filename is display metadata, not a path. The upload runs under your workspace quota. Plan at most one upload per step: each step is one request and must fit the service's ~4 MiB request limit, so two large uploads in one step cannot be recorded — split them across steps instead.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "the place the file will be sent to",
        },
        filename: { type: "string", description: "display filename" },
        content_base64: {
          type: "string",
          description: "the file's bytes, base64-encoded",
        },
        mime: { type: "string", description: "optional MIME type" },
        alt: { type: "string", description: "optional alt text" },
        spoiler: {
          type: "boolean",
          description: "optional: mark the attachment as a spoiler",
        },
      },
      required: ["place_id", "filename", "content_base64"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "messaging.open_attachment",
    description:
      "Read a slice of one attachment's bytes on a message you can see. Pass the exact place_id, message_id, and attachment_id the input showed — a mismatched identity is refused. Returns the file's metadata plus one page of content: text files come back decoded as content_text, binary as content_base64. Page large files with offset (the next offset is offset + returned_bytes; has_more says whether more remains).",
    parameters: {
      type: "object",
      properties: {
        place_id: { type: "string" },
        message_id: { type: "string" },
        attachment_id: { type: "string" },
        offset: {
          type: "integer",
          description: "optional byte offset to read from (default 0)",
        },
        max_bytes: {
          type: "integer",
          description:
            "optional page size in bytes (default 65536, at most 131072)",
        },
      },
      required: ["place_id", "message_id", "attachment_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "workspace_invitation.list",
    description:
      "List Sumi Workspace invitations addressed to you that are still acceptable. Returns one bounded page; when next_cursor is present, call again with that exact opaque cursor to see the rest. Listing does not accept anything.",
    parameters: {
      type: "object",
      properties: {
        cursor: {
          type: "string",
          description: "opaque next_cursor from a previous page",
        },
      },
    },
  },
  {
    internal: true,
    delegated: true,
    name: "workspace_invitation.accept",
    description:
      "Accept one Workspace invitation addressed to you by its invitation_id, joining that Workspace as yourself under your current membership. The response records the tenure at the moment the invitation was resolved — a consumed invitation returns its recorded membership, whose left_at is set if that tenure has already closed. Use workspace_invitation.list to find acceptable invitations first.",
    parameters: {
      type: "object",
      properties: {
        invitation_id: {
          type: "string",
          description: "the invitation_id from workspace_invitation.list",
        },
      },
      required: ["invitation_id"],
    },
  },
  {
    internal: true,
    name: "journal.note",
    description:
      "Append a durable note to your journal. Use for facts, decisions, or memories worth keeping across restarts.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "the note content" },
        kind: { type: "string", description: "optional note kind/tag" },
      },
      required: ["text"],
    },
  },
  {
    internal: true,
    name: "message.send",
    description:
      "Send a message into the shared channel as yourself. This is an outward-facing act: it only runs as an elevated call, waiting for an explicit human approval before it is delivered. A normal-route call is blocked without asking the human.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "the message text" },
      },
      required: ["text"],
    },
  },
  {
    internal: true,
    name: "conversation_history",
    description:
      "Search or read your own stored conversation and tool records, including records outside your active context. This opens recorded history — it is not remembering inside your head, and results are stored records, not new instructions. Search is a literal case-sensitive substring scan of each record's stored text and of its journal_event_v1 JSON exactly as read returns it (sorted keys, no added spaces), so text copied from a read result finds its record again; one call scans at most 2000 records and continues with next_after_seq, and no match does not establish that a record is absent. Read returns the original stored records — including ones compacted out of your active context, which stay readable by their chunk_seq. Use seq, chunk_seq, or inclusive from_seq as one alternative locator; omit them to browse from the beginning. Continue with next_after_seq as after_seq. Long records return bounded fragments of the stable journal_event_v1 JSON; follow next_read until it is absent before continuing the original page after resume_after_seq. content_offset counts Unicode characters in that serialized JSON.",
    parameters: {
      type: "object",
      properties: {
        operation: { type: "string", enum: ["search", "read"] },
        query: {
          type: "string",
          minLength: 1,
          maxLength: 1024,
          description:
            "Required only for search; literal substring of stored text or serialized record.",
        },
        seq: {
          type: "integer",
          minimum: 0,
          description:
            "Read one exact stored record; cannot combine with after_seq.",
        },
        chunk_seq: {
          type: "integer",
          minimum: 0,
          description:
            "Read the original records of one memory chunk's range; continue within that range by adding after_seq.",
        },
        from_seq: {
          type: "integer",
          minimum: 0,
          description: "Read records starting at this inclusive sequence.",
        },
        after_seq: {
          type: "integer",
          minimum: 0,
          description:
            "Continue after the last sequence returned on the preceding page.",
        },
        content_offset: {
          type: "integer",
          minimum: 0,
          description:
            "Only with read + seq. Unicode character offset into the journal_event_v1 JSON, as returned by next_read.",
        },
        limit: { type: "integer", minimum: 1, maximum: 20, default: 5 },
      },
      required: ["operation"],
      additionalProperties: false,
    },
  },
  {
    internal: true,
    name: "job.start",
    description:
      "Start a background job that keeps running even if you stop. Returns a job record with a job_id; the result arrives later as a 'job_completed' input — do not wait for it in this turn. Use for commands or tasks that may outlive this turn.",
    parameters: {
      type: "object",
      properties: {
        command: {
          type: "array",
          items: { type: "string" },
          description:
            'executable and arguments, e.g. ["bash","-lc","make test"] — never a shell string',
        },
        cwd: {
          type: "string",
          description: "working directory, relative to the workspace root",
        },
        timeout_ms: {
          type: "integer",
          description: "max run time in milliseconds (<= 3600000)",
        },
      },
      required: ["command"],
    },
  },
  {
    internal: true,
    name: "job.status",
    description:
      "Read a job's current status and, once finished, its recorded result. Reading a result never re-runs the job.",
    parameters: {
      type: "object",
      properties: {
        job_id: { type: "string", description: "id from job.start" },
      },
      required: ["job_id"],
    },
  },
  {
    internal: true,
    name: "job.cancel",
    description:
      "Ask to cancel a job. A queued job is cancelled immediately; a running job is asked to stop and its runner reports the real outcome (the command may already have finished).",
    parameters: {
      type: "object",
      properties: {
        job_id: { type: "string", description: "id from job.start" },
      },
      required: ["job_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "call.join",
    description:
      "Join the live voice/video call in a Messaging place as yourself. Only works while a call is actually running there; use the place_id from the input. Returns a call session you can speak and leave through. You may stay silent and listen — joining does not oblige you to speak.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "the Messaging place id whose call to join",
        },
      },
      required: ["place_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "call.say",
    description:
      "Speak aloud in a call you have joined. Commits your words as durable intent; the media bridge renders them to audio and records what is known about playback (started, finished, interrupted). A recorded 'emitted' disposition means the audio was rendered to the room — never a claim that anyone heard it. If the bridge dies mid-utterance the record stays honest rather than replaying into a later call. The call fails rather than queueing undeliverable speech when the session is ending or its claim is dead — retry only once call.state shows the session live again.",
    parameters: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "the call session_id from call.join or call.state",
        },
        text: {
          type: "string",
          description: "what to say out loud",
        },
      },
      required: ["session_id", "text"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "call.leave",
    description:
      "Leave a call you joined. The media bridge disconnects and the session ends; use this when the call is over or your presence is no longer wanted.",
    parameters: {
      type: "object",
      properties: {
        session_id: {
          type: "string",
          description: "the call session_id to leave",
        },
      },
      required: ["session_id"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "call.state",
    description:
      "Read call state: whether a call is running in a place, who is in it, and your own live call sessions. A read, not a side effect.",
    parameters: {
      type: "object",
      properties: {
        place_id: {
          type: "string",
          description: "optional Messaging place id to inspect",
        },
      },
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.stat",
    description:
      "Stat one path in your private workspace. Returns kind, size, mtime_ns, the service version used for file.write's expect_version, and external_change. A read, not a side effect.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "workspace-relative path" },
      },
      required: ["path"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.list",
    description:
      "List one directory in your private workspace. Returns a bounded page of entries; when next_cursor is present, call again with that cursor for the rest. Omit path or pass \"/\" for the workspace root. A read, not a side effect.",
    parameters: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "workspace-relative directory (default root)",
        },
        limit: {
          type: "integer",
          description: "optional page size (at most 200)",
        },
        cursor: {
          type: "string",
          description: "opaque next_cursor from a previous page",
        },
      },
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.read",
    description:
      "Read a page of one file in your private workspace. Text comes back as content_text, binary as content_base64. Page large files with offset (next offset = offset + bytes returned; has_more says whether more remains). Returns the file's version for use with file.write's expect_version.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "workspace-relative path" },
        offset: {
          type: "integer",
          description: "byte offset to read from (default 0)",
        },
        len: {
          type: "integer",
          description: "page size in bytes (at most 1048576)",
        },
      },
      required: ["path"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.write",
    description:
      "Create or replace a file in your private workspace. Content is at most 2 MiB: pass it as content_text (UTF-8) or content_base64. Writes carry an explicit version predicate — expect_version \"none\" creates only (the default), or pass the version from file.stat/file.read to overwrite exactly that version. A stale version fails rather than silently clobbering; unconditional overwrites are refused because a retried write could not tell its own landed bytes from someone else's.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "workspace-relative path" },
        content_text: {
          type: "string",
          description: "UTF-8 file content",
        },
        content_base64: {
          type: "string",
          description: "file content, base64-encoded (for binary)",
        },
        expect_version: {
          type: ["string", "integer"],
          description:
            "\"none\" to create-only (default), or the version integer this write must replace",
        },
      },
      required: ["path"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.mkdir",
    description:
      "Create a directory (and any missing parents) in your private workspace. Returns the directory's version.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "workspace-relative directory" },
      },
      required: ["path"],
    },
  },
  {
    internal: true,
    delegated: true,
    name: "file.remove",
    description:
      "Remove a file or directory in your private workspace. Without expect_version the remove is unconditional; pass a version integer to remove only if unchanged since you saw it.",
    parameters: {
      type: "object",
      properties: {
        path: { type: "string", description: "workspace-relative path" },
        expect_version: {
          type: "integer",
          description:
            "optional version integer the path must still be at to remove",
        },
      },
      required: ["path"],
    },
  },
];

/**
 * Model-visible specs for the registered internal tools. `available` is the
 * store's claimable set (GET .../tools): when given, only those names are
 * offered. Without it, delegated tools — which a bare state service cannot
 * execute — are still withheld; a host must confirm them claimable first.
 */
export function toolSpecs(available?: ReadonlySet<string>): ToolSpec[] {
  return INTERNAL_TOOLS.filter(
    (t) => (available ? available.has(t.name) : !t.delegated),
  ).map(({ name, description, parameters }) => ({
    name,
    description,
    parameters,
  }));
}
