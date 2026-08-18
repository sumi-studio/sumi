import { readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { messagingPlacePath } from "../../public/place-path.js";
import { placePath } from "./place-route";

describe("placePath", () => {
  it.each([
    ["channel:channel-1", "/w/workspace-1/messaging/c/channel-1"],
    ["dm:dm-1", "/w/workspace-1/messaging/dm/dm-1"],
    ["group_dm:group-1", "/w/workspace-1/messaging/group/group-1"],
  ] as const)("keeps %s inside its exact Workspace", (place, expected) => {
    expect(placePath("workspace-1", place)).toBe(expected);
  });
});

/**
 * ファイル名から route を読む。@tanstack/router のファイル規約そのままで、
 * `w.$workspaceId.messaging.c.$channelId.tsx` は
 * `/w/$workspaceId/messaging/c/$channelId` を意味する。
 */
function routePatterns(): string[] {
  const dir = fileURLToPath(new URL("../routes", import.meta.url));
  return readdirSync(dir)
    .filter((name) => name.endsWith(".tsx") && !name.includes(".test."))
    .map((name) => name.replace(/\.tsx$/, ""))
    .filter((name) => !name.startsWith("__") && !name.startsWith("-"))
    .map((name) => `/${name.split(".").join("/")}`)
    .map((path) => path.replace(/\/index$/, ""));
}

function matchesSomeRoute(path: string, patterns: string[]): boolean {
  const segments = path.split("/");
  return patterns.some((pattern) => {
    const expected = pattern.split("/");
    if (expected.length !== segments.length) return false;
    return expected.every(
      (segment, index) =>
        segment.startsWith("$") || segment === segments[index],
    );
  });
}

/**
 * 通知が指す住所は、アプリが実際に持っている route でなければならない。
 * ここが緩むと「押しても開かない通知」になり、実際に一度そうなった——SW が
 * `/c/<id>` を手で組み立てていた一方で、route は
 * `/w/<workspaceId>/messaging/c/<id>` に移っていた。
 *
 * だから住所の作り方は public/place-path.js の一つだけで、アプリのルーターも
 * Service Worker もそれを通る。ここで見るのは、その一つが route の定義
 * （src/routes のファイル）と一致していることである。
 */
describe("the one place-path rule the app and the service worker share", () => {
  const patterns = routePatterns();

  it("has the app's own routes to match against", () => {
    // 下の一致が「route を一つも読めていないから通った」ではないこと。
    expect(patterns).toContain("/w/$workspaceId/messaging/c/$channelId");
  });

  it.each([
    ["channel", "channel-1"],
    ["dm", "dm-1"],
    ["group_dm", "group-1"],
  ] as const)("builds a %s address the router can open", (kind, id) => {
    expect(
      matchesSomeRoute(messagingPlacePath("workspace-1", kind, id), patterns),
    ).toBe(true);
  });

  it("falls back to the Workspace's Messaging screen, which is also a real route", () => {
    const base = messagingPlacePath("workspace-1", "unknown-kind", "x");
    expect(base).toBe("/w/workspace-1/messaging");
    expect(matchesSomeRoute(base, patterns)).toBe(true);
  });
});
