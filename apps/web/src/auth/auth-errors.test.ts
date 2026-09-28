import { describe, expect, it } from "vitest";
import { getAuthErrorMessage } from "./auth-errors";
import { AuthAPIError } from "./session-client";

function message(code: string, status: number): string {
  return getAuthErrorMessage(new AuthAPIError(code, status));
}

describe("invitation recovery guidance", () => {
  it("keeps GitHub usable when GitHub has not verified the invited address", () => {
    const text = message("invitation_email_unverified", 403);
    expect(text).toContain("Settings → Emails");
    expect(text).toContain("同じ方法で続けて");
    expect(text).toContain("招待はそのまま使えます");
    expect(text).not.toContain("Google");
    expect(text).not.toContain("利用対象");
  });

  it("asks for GitHub's answer again instead of another provider", () => {
    const text = message("invitation_email_proof_required", 403);
    expect(text).toContain("もう一度GitHubで続け");
    expect(text).toContain("招待はそのまま使えます");
    expect(text).not.toContain("Google");
  });

  it("reports a GitHub outage as retryable, not as a refusal", () => {
    const text = message("invitation_email_proof_unavailable", 503);
    expect(text).toContain("少し時間をおいて");
    expect(text).not.toContain("ログイン機能は現在利用できません");
  });

  it("explains a different address and how to keep using GitHub", () => {
    const text = message("invitation_email_mismatch", 403);
    expect(text).toContain("招待を受け取ったアドレス");
    expect(text).toContain("GitHubに追加");
  });

  it("keeps an invalid invitation separate from a usable invitation needing email proof", () => {
    const text = message("invitation_required", 403);
    expect(text).toContain("招待リンク");
    expect(text).not.toContain("そのまま使えます");
  });
});
