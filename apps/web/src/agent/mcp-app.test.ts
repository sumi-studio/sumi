import { describe, expect, it } from "vitest";
import {
  checkMcpAppProjectionIntegrity,
  MAX_MCP_APP_HTML_BYTES,
  parseMcpAppProjectionCandidate,
  resolveMcpAppSandboxConfig,
} from "./mcp-app";

const html = "<!doctype html><html><body>trusted</body></html>";
const hash =
  "sha256:fd1e7c1c23bd6a474f5f49fea040b84f37dfabf4baf832c89afd687d16624b7e";

function projection(overrides: Record<string, unknown> = {}) {
  return {
    kind: "mcp_app_projection_candidate",
    claimedSource: {
      serverId: "calendar-server",
      toolName: "show-calendar",
      resourceUri: "ui://calendar/view",
      resourceSha256: hash,
    },
    resource: {
      uri: "ui://calendar/view",
      mimeType: "text/html;profile=mcp-app",
      text: html,
      csp: {
        resourceDomains: ["https://cdn.example"],
      },
    },
    toolInput: { month: "2026-07" },
    toolResult: {
      content: [{ type: "text", text: "done" }],
      structuredContent: { events: [] },
    },
    ...overrides,
  };
}

describe("MCP App projection candidate", () => {
  it("does not activate from an unprovenanced generic tool result", () => {
    expect(
      parseMcpAppProjectionCandidate({
        content: [{ type: "text", text: html }],
        structuredContent: { html },
      }),
    ).toBeNull();
    expect(
      parseMcpAppProjectionCandidate({
        type: "mcp_app",
        resource: { uri: "ui://calendar/view", text: html },
      }),
    ).toBeNull();
  });

  it("treats self-described source fields as claims and checks only content integrity", async () => {
    const parsed = parseMcpAppProjectionCandidate(projection());
    expect(parsed).not.toBeNull();
    if (!parsed) {
      throw new Error("projection should parse");
    }
    await expect(checkMcpAppProjectionIntegrity(parsed)).resolves.toMatchObject(
      {
        html,
        claimedSource: {
          serverId: "calendar-server",
          toolName: "show-calendar",
          resourceUri: "ui://calendar/view",
          resourceSha256: hash,
        },
      },
    );

    const badHash = parseMcpAppProjectionCandidate(
      projection({
        claimedSource: {
          serverId: "calendar-server",
          toolName: "show-calendar",
          resourceUri: "ui://calendar/view",
          resourceSha256: `sha256:${"0".repeat(64)}`,
        },
      }),
    );
    expect(badHash).not.toBeNull();
    if (!badHash) {
      throw new Error(
        "projection with a syntactically valid hash should parse",
      );
    }
    await expect(checkMcpAppProjectionIntegrity(badHash)).resolves.toBeNull();
  });

  it("records no permission grant even when the resource requests one", () => {
    const parsed = parseMcpAppProjectionCandidate(
      projection({
        resource: {
          uri: "ui://calendar/view",
          mimeType: "text/html;profile=mcp-app",
          text: html,
          permissions: { camera: {} },
        },
      }),
    );
    expect(parsed).not.toBeNull();
    expect(parsed?.resource).not.toHaveProperty("permissions");
  });

  it("rejects non-exact CSP origins and oversize HTML", () => {
    expect(
      parseMcpAppProjectionCandidate(
        projection({
          resource: {
            uri: "ui://calendar/view",
            mimeType: "text/html;profile=mcp-app",
            text: html,
            csp: { connectDomains: ["https://example.test/path"] },
          },
        }),
      ),
    ).toBeNull();

    expect(
      parseMcpAppProjectionCandidate(
        projection({
          resource: {
            uri: "ui://calendar/view",
            mimeType: "text/html;profile=mcp-app",
            text: "x".repeat(MAX_MCP_APP_HTML_BYTES + 1),
          },
        }),
      ),
    ).toBeNull();
  });

  it("accepts protocol-defined secure websocket and wildcard resource sources", () => {
    expect(
      parseMcpAppProjectionCandidate(
        projection({
          resource: {
            uri: "ui://calendar/view",
            mimeType: "text/html;profile=mcp-app",
            text: html,
            csp: {
              connectDomains: ["wss://realtime.example"],
              resourceDomains: ["https://*.cdn.example"],
            },
          },
        }),
      ),
    ).not.toBeNull();
  });
});

describe("MCP App sandbox configuration", () => {
  const deploymentId = "a".repeat(64);

  it("requires explicit HTTPS cross-origin URL and a deployment marker", () => {
    expect(
      resolveMcpAppSandboxConfig(
        undefined,
        deploymentId,
        "https://sumi.example/chat",
      ),
    ).toBeNull();
    expect(
      resolveMcpAppSandboxConfig(
        "/mcp-app-sandbox.html",
        deploymentId,
        "https://sumi.example/chat",
      ),
    ).toBeNull();
    expect(
      resolveMcpAppSandboxConfig(
        "https://sumi.example/mcp-app-sandbox.html",
        deploymentId,
        "https://sumi.example/chat",
      ),
    ).toBeNull();
    expect(
      resolveMcpAppSandboxConfig(
        "http://sandbox.example/mcp-app-sandbox.html",
        deploymentId,
        "https://sumi.example/chat",
      ),
    ).toBeNull();
  });

  it("binds the exact host origin and deployment marker to the sandbox URL", () => {
    expect(
      resolveMcpAppSandboxConfig(
        "https://sandbox.example/mcp-app-sandbox.html",
        deploymentId,
        "https://sumi.example/chat?ignored=true",
      ),
    ).toEqual({
      url: `https://sandbox.example/mcp-app-sandbox.html?hostOrigin=https%3A%2F%2Fsumi.example&deploymentId=${deploymentId}`,
      origin: "https://sandbox.example",
      deploymentId,
    });
  });
});
