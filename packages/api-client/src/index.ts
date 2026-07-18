import type { components } from "./generated/schema";

export type { components, paths } from "./generated/schema";

// WebSocket イベント (REST からは参照されないため components 経由で公開する)
export type ClientEvent = components["schemas"]["ClientEvent"];
export type ServerEvent = components["schemas"]["ServerEvent"];
