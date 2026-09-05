import assert from "node:assert/strict";
import test from "node:test";
import {
  executorAuthorityProbeFile,
  firstProviderResponse,
  firstUserMessage,
  LoopbackChatProvider,
  secondProviderResponse,
  secondUserMessage,
} from "../e2e/support/real-agent-stack";

const invitationID = "0198f0f4-9b72-7000-8000-000000000811";
const workspaceID = "0198f0f4-9b72-7000-8000-000000000011";
const workspaceMemberID = "0198f0f4-9b72-7000-8000-000000000911";
const personalityAgentID = "0198f0f4-9b72-7000-8000-000000000711";
const workspaceName = "Provider Fixture Workspace";

function providerToolCall(
  id: string,
  name: string,
  input: Record<string, unknown>,
) {
  return {
    id,
    type: "function",
    function: {
      name,
      arguments: JSON.stringify({ route: "normal", input }),
    },
  };
}

function providerToolDefinition(name: string) {
  return {
    type: "function",
    function: {
      name,
      parameters: {
        type: "object",
        properties: {
          route: { type: "string", enum: ["normal", "elevated"] },
          input: { type: "object", properties: {} },
        },
      },
    },
  };
}

async function completion(
  provider: LoopbackChatProvider,
  messages: unknown[],
  tools: unknown[] = [],
): Promise<Response> {
  return fetch(`${provider.url}/chat/completions`, {
    method: "POST",
    headers: {
      Authorization: "Bearer test-provider-key",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model: "kimi-k2.7-code",
      stream: true,
      messages,
      tools,
    }),
  });
}

test("loopback provider counts only requests that pass transport validation", async () => {
  const provider = new LoopbackChatProvider("test-provider-key");
  await provider.start();
  try {
    assert.equal((await fetch(`${provider.url}/probe`)).status, 404);
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        })
      ).status,
      401,
    );
    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "text/plain",
          },
          body: "{}",
        })
      ).status,
      415,
    );
    assert.equal(provider.requestCount, 0);

    assert.equal(
      (
        await fetch(`${provider.url}/chat/completions`, {
          method: "POST",
          headers: {
            Authorization: "Bearer test-provider-key",
            "Content-Type": "application/json",
          },
          body: "{}",
        })
      ).status,
      422,
    );
    assert.equal(provider.requestCount, 1);
  } finally {
    await provider.stop();
  }
});

test("loopback provider verifies membership and conversation context", () =>
  verifyMembershipAndContext(false));

test("loopback provider accepts the current Messaging overview", () =>
  verifyMembershipAndContext(true));

async function verifyMembershipAndContext(withMessaging: boolean) {
  const provider = new LoopbackChatProvider("test-provider-key");
  await provider.start();
  try {
    const messages: unknown[] = [{ role: "user", content: firstUserMessage }];
    const tools = [
      providerToolDefinition("workspace_invitation_list"),
      providerToolDefinition("workspace_invitation_accept"),
      providerToolDefinition("workspace_list"),
      providerToolDefinition("list_dir"),
    ];
    const invitationListResponse = await completion(provider, messages, tools);
    assert.equal(invitationListResponse.status, 200);
    assert.match(
      await invitationListResponse.text(),
      /call-real-agent-invitation-list/,
    );

    const invitationListCall = providerToolCall(
      "call-real-agent-invitation-list",
      "workspace_invitation_list",
      {},
    );
    messages.push(
      { role: "assistant", tool_calls: [invitationListCall] },
      {
        role: "tool",
        tool_call_id: invitationListCall.id,
        content: JSON.stringify({
          invitations: [
            {
              invitation_id: invitationID,
              workspace_id: workspaceID,
              workspace_name: workspaceName,
              created_at: "2026-08-16T00:00:00Z",
              expires_at: "2026-08-17T00:00:00Z",
            },
          ],
        }),
      },
    );
    const invitationAcceptResponse = await completion(provider, messages);
    assert.equal(invitationAcceptResponse.status, 200);
    assert.match(
      await invitationAcceptResponse.text(),
      /call-real-agent-invitation-accept/,
    );

    const invitationAcceptCall = providerToolCall(
      "call-real-agent-invitation-accept",
      "workspace_invitation_accept",
      { invitation_id: invitationID },
    );
    messages.push(
      { role: "assistant", tool_calls: [invitationAcceptCall] },
      {
        role: "tool",
        tool_call_id: invitationAcceptCall.id,
        content: JSON.stringify({
          workspace_member_id: workspaceMemberID,
          workspace_id: workspaceID,
          display_name: "Fixture PersonalityAgent",
          owner: false,
          role_ids: [],
          joined_at: "2026-08-16T00:01:00Z",
          left_at: null,
        }),
      },
    );
    const workspaceListResponse = await completion(provider, messages);
    assert.equal(workspaceListResponse.status, 200);
    assert.match(
      await workspaceListResponse.text(),
      /call-real-agent-workspace-list/,
    );

    const workspaceListCall = providerToolCall(
      "call-real-agent-workspace-list",
      "workspace_list",
      {},
    );
    messages.push(
      { role: "assistant", tool_calls: [workspaceListCall] },
      {
        role: "tool",
        tool_call_id: workspaceListCall.id,
        content: JSON.stringify({
          workspaces: [{ workspace_id: workspaceID, name: workspaceName }],
        }),
      },
    );
    const executorCallResponse = await completion(provider, messages);
    assert.equal(executorCallResponse.status, 200);
    assert.match(await executorCallResponse.text(), /call-real-agent-list-dir/);

    const executorCall = providerToolCall(
      "call-real-agent-list-dir",
      "list_dir",
      { path: "." },
    );
    messages.push(
      { role: "assistant", tool_calls: [executorCall] },
      {
        role: "tool",
        tool_call_id: executorCall.id,
        content: executorAuthorityProbeFile,
      },
    );
    const firstTextResponse = await completion(provider, messages);
    assert.equal(firstTextResponse.status, 200);
    assert.match(
      await firstTextResponse.text(),
      new RegExp(firstProviderResponse),
    );
    assert.equal(provider.invitationListVerified, true);
    assert.equal(provider.invitationAcceptVerified, true);
    assert.equal(provider.workspaceMembershipVerified, true);
    assert.equal(provider.invitationID, invitationID);
    assert.equal(provider.workspaceID, workspaceID);
    assert.equal(provider.workspaceName, workspaceName);
    assert.equal(provider.executorToolVerified, true);

    messages.push(
      { role: "assistant", content: firstProviderResponse },
      { role: "user", content: secondUserMessage },
    );
    const secondTextResponse = await completion(
      provider,
      messages,
      withMessaging ? [providerToolDefinition("messaging")] : [],
    );
    assert.equal(secondTextResponse.status, 200);
    if (withMessaging) {
      assert.match(
        await secondTextResponse.text(),
        /call-real-agent-messaging-overview/,
      );
      const overviewCall = providerToolCall(
        "call-real-agent-messaging-overview",
        "messaging",
        { workspace_id: workspaceID, action: "overview" },
      );
      const channelID = "0198f0f4-9b72-7000-8000-000000000021";
      messages.push(
        { role: "assistant", tool_calls: [overviewCall] },
        {
          role: "tool",
          tool_call_id: overviewCall.id,
          content: JSON.stringify({
            workspaces: [{ workspace_id: workspaceID, name: workspaceName }],
            channels: [
              {
                channel_id: channelID,
                workspace_id: workspaceID,
                name: "attachments",
                topic: "",
                visibility: "public",
                voice: false,
                revision: 1,
              },
            ],
            dms: [],
            threads: [],
            members: [],
            read_markers: [],
            reply_later_markers: [],
            unread_summaries: [],
            self: {
              kind: "personality_agent",
              personality_agent_id: personalityAgentID,
            },
          }),
        },
      );
      const openResponse = await completion(provider, messages);
      assert.equal(openResponse.status, 200, await openResponse.clone().text());
      const open = await openResponse.text();
      assert.match(open, /call-real-agent-messaging-open-human/);
      assert.ok(open.includes(channelID));
      assert.equal(provider.requestCount, 7);
    } else {
      assert.match(
        await secondTextResponse.text(),
        new RegExp(secondProviderResponse),
      );
      assert.equal(provider.requestCount, 6);
    }
    assert.equal(provider.contextVerified, true);
  } finally {
    await provider.stop();
  }
}
