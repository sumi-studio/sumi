// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { MemberProfile, ParticipantKey } from "../model";
import { MessageContent } from "./message-content";

const members: Record<ParticipantKey, MemberProfile> = {
  "human:haku": {
    participant: { kind: "human", humanId: "haku" },
    displayName: "Haku",
    tagline: "founder",
  },
  "personality_agent:sumi": {
    participant: { kind: "personality_agent", personalityAgentId: "sumi" },
    displayName: "スミ",
    tagline: "agent",
  },
};

const selfKey: ParticipantKey = "human:haku";

function renderContent(
  content: string,
  trailer?: { text: string; title?: string },
) {
  return render(
    <MessageContent
      content={content}
      members={members}
      selfKey={selfKey}
      trailer={trailer}
    />,
  );
}

afterEach(cleanup);

describe("MessageContent markdown", () => {
  it("renders a fenced code block with language label and copy button", () => {
    const { container, getByText, getByRole } = renderContent(
      "before\n```ts\nconst a = 1;\n```",
    );
    const pre = container.querySelector("pre");
    expect(pre).not.toBeNull();
    expect(pre).toHaveTextContent("const a = 1;");
    expect(getByText("ts")).toBeInTheDocument();
    expect(getByRole("button", { name: "コードをコピー" })).toBeInTheDocument();
  });

  it("copies the code body when the copy button is clicked", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    try {
      const { getByRole } = renderContent("```\nconst a = 1;\n```");
      getByRole("button", { name: "コードをコピー" }).click();
      expect(writeText).toHaveBeenCalledWith("const a = 1;");
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("renders inline code, bold, strikethrough, quote and lists", () => {
    const { container } = renderContent(
      "`inline` **太字** ~~消す~~\n\n> 引用\n\n- 一\n- 二\n\n1. 甲",
    );
    expect(container.querySelector("code")).toHaveTextContent("inline");
    expect(container.querySelector("strong")).toHaveTextContent("太字");
    expect(container.querySelector("del")).toHaveTextContent("消す");
    expect(container.querySelector("blockquote")).toHaveTextContent("引用");
    expect(container.querySelector("ul")).toHaveTextContent("一");
    expect(container.querySelector("ol")).toHaveTextContent("甲");
  });

  it("keeps bold working next to CJK punctuation", () => {
    const { container } = renderContent("これは**「重要」**です");
    expect(container.querySelector("strong")).toHaveTextContent("「重要」");
  });

  it("renders headings subdued: no h1..h6 elements", () => {
    const { container } = renderContent("# 見出し");
    expect(container.querySelector("h1,h2,h3,h4,h5,h6")).toBeNull();
    expect(container).toHaveTextContent("見出し");
  });

  it("turns a single newline into a line break", () => {
    const { container } = renderContent("一行目\n二行目");
    expect(container.querySelector("br")).not.toBeNull();
  });
});

describe("MessageContent mentions and links", () => {
  it("decorates mentions, with amber for self", () => {
    const { getByText } = renderContent("@Haku と @スミ を呼ぶ");
    const self = getByText("@Haku");
    const other = getByText("@スミ");
    expect(self).toHaveAttribute("data-mention", "self");
    expect(self.className).toContain("amber");
    expect(other).toHaveAttribute("data-mention", "other");
    expect(other.className).toContain("text-primary");
  });

  it("linkifies bare URLs with safe rel/target", () => {
    const { container } = renderContent("see https://example.com/x?a=1 now");
    const anchor = container.querySelector("a");
    expect(anchor).toHaveAttribute("href", "https://example.com/x?a=1");
    expect(anchor).toHaveAttribute("target", "_blank");
    expect(anchor).toHaveAttribute("rel", "noreferrer noopener");
  });

  it("renders markdown links with safe rel/target", () => {
    const { container } = renderContent("[docs](https://example.com/docs)");
    const anchor = container.querySelector("a");
    expect(anchor).toHaveAttribute("href", "https://example.com/docs");
    expect(anchor).toHaveAttribute("rel", "noreferrer noopener");
  });

  it("does not decorate mentions or URLs inside code blocks", () => {
    const { container } = renderContent("```\n@Haku https://example.com\n```");
    expect(container.querySelector("[data-mention]")).toBeNull();
    expect(container.querySelector("a")).toBeNull();
    expect(container.querySelector("pre")).toHaveTextContent(
      "@Haku https://example.com",
    );
  });

  it("does not decorate mentions inside inline code", () => {
    const { container } = renderContent("`@Haku` へ");
    expect(container.querySelector("[data-mention]")).toBeNull();
    expect(container.querySelector("code")).toHaveTextContent("@Haku");
  });

  it("decorates mentions outside code while leaving code untouched", () => {
    const { container, getByText } = renderContent(
      "@スミ これ見て\n```\n@スミ ignore\n```",
    );
    expect(getByText("@スミ")).toHaveAttribute("data-mention", "other");
    expect(container.querySelectorAll("[data-mention]")).toHaveLength(1);
  });

  it("decorates mentions restored from currency text before rendering real math", () => {
    const { container, getByText } = renderContent(
      "@Haku の予算は $5 と $10、式は $x + 1$",
    );

    expect(getByText("@Haku")).toHaveAttribute("data-mention", "self");
    expect(container.querySelectorAll(".katex")).toHaveLength(1);
    expect(container).toHaveTextContent("$5 と $10");
  });
});

describe("MessageContent sanitization", () => {
  it("never renders raw HTML from the body (img onerror)", () => {
    const { container } = renderContent(
      '<img src=x onerror="alert(1)"> <script>alert(2)</script>',
    );
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("script")).toBeNull();
  });

  it("never fetches a markdown image: renders a link instead of <img>", () => {
    const { container } = renderContent(
      "![pixel](https://attacker.example/pixel.png)",
    );
    expect(container.querySelector("img")).toBeNull();
    const link = container.querySelector("[data-image-link]");
    expect(link).toHaveAttribute("href", "https://attacker.example/pixel.png");
    expect(link).toHaveAttribute("rel", "noreferrer noopener");
    expect(link).toHaveTextContent("pixel");
  });

  it("never fetches a reference-style markdown image either", () => {
    const { container } = renderContent(
      "![pixel][p]\n\n[p]: https://attacker.example/ref.png",
    );
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("[data-image-link]")).toHaveAttribute(
      "href",
      "https://attacker.example/ref.png",
    );
  });

  it("does not build an image link from an unsafe URL scheme", () => {
    const { container } = renderContent("![x](javascript:alert(1))");
    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("[data-image-link]")).toBeNull();
    expect(container.innerHTML).not.toContain("javascript:");
  });

  it("does not execute javascript: URLs are left as plain text, not mangled HTML", () => {
    const { container } = renderContent("<a href='javascript:alert(1)'>x</a>");
    const anchor = container.querySelector("a");
    if (anchor) {
      expect(anchor.getAttribute("href") ?? "").not.toContain("javascript:");
    }
  });
});

describe("MessageContent trailer", () => {
  it("appends the edited label inline inside the last paragraph", () => {
    const { container } = renderContent("直した", {
      text: "(編集済み)",
      title: "8月4日 12:00",
    });
    const paragraph = container.querySelector("p");
    const trailer = paragraph?.querySelector("[data-trailer]");
    expect(trailer).toHaveTextContent("(編集済み)");
    expect(trailer).toHaveAttribute("title", "8月4日 12:00");
  });

  it("appends the edited label after a trailing code block", () => {
    const { container } = renderContent("```\nx\n```", {
      text: "(編集済み)",
    });
    const trailer = container.querySelector("[data-trailer]");
    expect(trailer).toHaveTextContent("(編集済み)");
    expect(trailer?.closest("pre")).toBeNull();
  });
});
