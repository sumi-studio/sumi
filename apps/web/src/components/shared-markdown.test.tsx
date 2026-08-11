// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  CompactMessageResponse,
  type CompactMessageResponseProps,
} from "@sumi/ui/ai-elements/compact-message-response";
import { cleanup, fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("@sumi/ui CompactMessageResponse", () => {
  it("keeps presentation and security overrides out of the public API", () => {
    type UnsafeOverride = Extract<
      "components" | "urlTransform" | "rehypePlugins",
      keyof CompactMessageResponseProps
    >;
    const hasUnsafeOverride: UnsafeOverride extends never ? false : true =
      false;

    expect(hasUnsafeOverride).toBe(false);
  });

  it("ignores an untyped attempt to replace the image policy", () => {
    const unsafeProps = {
      children: "![pixel](https://attacker.example/pixel.png)",
      components: {
        img: ({ src }: { src?: string }) => <img src={src} alt="unsafe" />,
      },
    } as unknown as CompactMessageResponseProps;
    const { container } = render(<CompactMessageResponse {...unsafeProps} />);

    expect(container.querySelector("img")).toBeNull();
    expect(container.querySelector("[data-image-link]")).toHaveAttribute(
      "href",
      "https://attacker.example/pixel.png",
    );
  });

  it("owns the common GFM, CJK and line-break parsing defaults", () => {
    const { container } = render(
      <CompactMessageResponse>{`これは**「重要」**です
次の行 ~~削除~~`}</CompactMessageResponse>,
    );

    expect(container.querySelector("strong")).toHaveTextContent("「重要」");
    expect(container.querySelector("del")).toHaveTextContent("削除");
    expect(container.querySelector("br")).not.toBeNull();
  });

  it("owns link, external-image and raw-HTML security policy", () => {
    const { container } = render(
      <CompactMessageResponse>{`[docs](https://example.com/docs)

![pixel](https://attacker.example/pixel.png)

![unsafe](javascript:alert(1))

<img src=x onerror="alert(2)"><script>alert(3)</script>`}</CompactMessageResponse>,
    );

    const docs = container.querySelector('a[href="https://example.com/docs"]');
    expect(docs).toHaveAttribute("target", "_blank");
    expect(docs).toHaveAttribute("rel", "noreferrer noopener");
    expect(container.querySelector("img,script")).toBeNull();
    expect(container.querySelector("[data-image-link]")).toHaveAttribute(
      "href",
      "https://attacker.example/pixel.png",
    );
    expect(container.innerHTML).not.toContain("javascript:");
  });

  it("owns compact code presentation and copy behavior", () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    vi.stubGlobal("navigator", { ...navigator, clipboard: { writeText } });
    const { container, getByRole, getByText } = render(
      <CompactMessageResponse>
        {"```ts\nconst x = 1;\n```"}
      </CompactMessageResponse>,
    );

    expect(container.querySelector("pre")).toHaveTextContent("const x = 1;");
    expect(getByText("ts")).toBeInTheDocument();
    getByRole("button", { name: "コードをコピー" }).click();
    expect(writeText).toHaveBeenCalledWith("const x = 1;");
  });

  it("appends a trailer through the shared boundary", () => {
    const { container } = render(
      <CompactMessageResponse
        trailer={{ text: "(編集済み)", title: "更新時刻" }}
      >
        本文
      </CompactMessageResponse>,
    );

    const paragraph = container.querySelector("p");
    expect(paragraph).toHaveTextContent("本文(編集済み)");
    expect(paragraph?.querySelector("[data-trailer]")).toHaveAttribute(
      "title",
      "更新時刻",
    );
  });

  it("does not memoize away trailer changes", () => {
    const { container, rerender } = render(
      <CompactMessageResponse trailer={{ text: "(旧)" }}>
        本文
      </CompactMessageResponse>,
    );

    rerender(
      <CompactMessageResponse trailer={{ text: "(新)" }}>
        本文
      </CompactMessageResponse>,
    );

    expect(container.querySelector("p")).toHaveTextContent("本文(新)");
    expect(container).not.toHaveTextContent("(旧)");
  });

  it("renders inline TeX with accessible MathML and keeps surrounding text inline", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"質量とエネルギーは $E = mc^2$ で結ばれる"}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).not.toBeNull();
    expect(container.querySelector(".katex-display")).toBeNull();
    expect(container.querySelector("[data-math-inline]")?.className).toContain(
      "overflow-x-auto",
    );
    expect(container.querySelector("math")?.getAttribute("aria-hidden")).toBe(
      null,
    );
    expect(container.querySelector(".katex-html")).toHaveAttribute(
      "aria-hidden",
      "true",
    );
    expect(
      container.querySelector('annotation[encoding="application/x-tex"]'),
    ).toHaveTextContent("E = mc^2");
    expect(container.querySelector("p")).toHaveTextContent(
      "質量とエネルギーは",
    );
    expect(container.querySelector("p")).toHaveTextContent("で結ばれる");
  });

  it("renders standalone double-dollar lines as contained display math", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"前の行\n$$\\sum_{i=1}^{n} i = \\frac{n(n+1)}{2}$$\n後の行"}
      </CompactMessageResponse>,
    );

    const display = container.querySelector(".katex-display");
    const wrapper = display?.parentElement;
    expect(display).not.toBeNull();
    expect(wrapper).toHaveAttribute("data-math-display");
    expect(wrapper?.className).toContain("max-w-full");
    expect(wrapper?.className).toContain("overflow-x-auto");
    expect(container.querySelector("pre")).toBeNull();
    expect(container).toHaveTextContent("前の行");
    expect(container).toHaveTextContent("後の行");
  });

  it("keeps double-dollar math inline when text shares its line", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"途中に $$x^2 + 1$$ を置く"}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).not.toBeNull();
    expect(container.querySelector(".katex-display")).toBeNull();
  });

  it("renders multiline double-dollar fences as display math", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"解は\n\n$$\n\\frac{-b \\pm \\sqrt{b^2 - 4ac}}{2a}\n$$"}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex-display")).not.toBeNull();
    expect(container.querySelector("[data-math-display]")).not.toBeNull();
    expect(container.querySelector("pre")).toBeNull();
  });

  it.each([
    "ランチは $5 と $10 でした",
    "コストは$5、利益は$3です",
    "US$ 100 と US$ 200",
    "$1,200 〜 $1,500",
  ])("leaves currency-like dollars as text: %s", (source) => {
    const { container } = render(
      <CompactMessageResponse>{source}</CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).toBeNull();
    expect(container).toHaveTextContent(source);
  });

  it("keeps later math and native Markdown after a currency opener", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"価格は $5。**重要** [資料](https://example.com) `code`、式は $x+1$"}
      </CompactMessageResponse>,
    );

    expect(container).toHaveTextContent("価格は $5。");
    expect(container.querySelector("strong")).toHaveTextContent("重要");
    expect(container.querySelector("a")).toHaveAttribute(
      "href",
      "https://example.com",
    );
    expect(container.querySelector("code")).toHaveTextContent("code");
    expect(container.querySelectorAll(".katex")).toHaveLength(1);
    expect(
      container.querySelector('annotation[encoding="application/x-tex"]'),
    ).toHaveTextContent("x+1");
  });

  it.each([
    "価格は $5、式は $x+1$",
    "$5 **bold** $x+1$",
    "$5 [link](https://example.com) $x+1$",
    "$5 `code` $x+1$",
  ])("does not let a price consume later structure: %s", (source) => {
    const { container } = render(
      <CompactMessageResponse>{source}</CompactMessageResponse>,
    );

    expect(container).toHaveTextContent("$5");
    expect(container.querySelectorAll(".katex")).toHaveLength(1);
    expect(
      container.querySelector('annotation[encoding="application/x-tex"]'),
    ).toHaveTextContent("x+1");
  });

  it("respects escaped delimiters and leaves unclosed math readable", () => {
    const { container } = render(
      <CompactMessageResponse>
        {String.raw`escaped \$x^2\$ and unclosed $y + 1`}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).toBeNull();
    expect(container).toHaveTextContent("escaped $x^2$ and unclosed $y + 1");
  });

  it("does not parse math delimiters inside code spans or fences", () => {
    const { container } = render(
      <CompactMessageResponse>
        {"`$x^2$`\n\n```tex\n$$y^2$$\n```"}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).toBeNull();
    expect(container.querySelector("code")).toHaveTextContent("$x^2$");
    expect(container.querySelector("pre")).toHaveTextContent("$$y^2$$");
  });

  it("shows malformed TeX as source text without aborting the message", () => {
    const { container } = render(
      <CompactMessageResponse>
        {String.raw`before $\frac{1}$ after`}
      </CompactMessageResponse>,
    );

    const fallback = container.querySelector(".katex-error");
    expect(fallback).toHaveTextContent(String.raw`\frac{1}`);
    expect(fallback).toHaveAttribute("title");
    expect(fallback).toHaveStyle({ color: "var(--destructive)" });
    expect(container).toHaveTextContent("before");
    expect(container).toHaveTextContent("after");
  });

  it("clamps author-controlled dimensions to KaTeX's finite maxSize", () => {
    const { container } = render(
      <CompactMessageResponse>
        {String.raw`$\rule{100000em}{100000em}$`}
      </CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).not.toBeNull();
    expect(container.querySelector(".katex-html")?.innerHTML).not.toContain(
      "100000em",
    );
    expect(container.querySelector(".katex-html")?.innerHTML).toContain("20em");
  });

  it("rejects deeply nested TeX before KaTeX and preserves exact source", () => {
    const tex = `${String.raw`\sqrt{`.repeat(65)}x${"}".repeat(65)}`;
    const { container } = render(
      <CompactMessageResponse>{`$${tex}$`}</CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).toBeNull();
    expect(
      container.querySelector("[data-math-fallback=depth]"),
    ).toHaveTextContent(tex);
  });

  it("rejects overlong TeX before rendering and keeps a scrollable fallback", () => {
    const tex = "x".repeat(4_097);
    const { container } = render(
      <CompactMessageResponse>{`$${tex}$`}</CompactMessageResponse>,
    );

    const fallback = container.querySelector("[data-math-fallback=length]");
    expect(fallback).toHaveTextContent(tex);
    expect(fallback?.parentElement).toHaveAttribute("data-math-inline");
    expect(fallback?.parentElement?.className).toContain("overflow-x-auto");
  });

  it("rejects excessive TeX token complexity below the input-size cap", () => {
    const tex = "x+".repeat(1_025);
    const { container } = render(
      <CompactMessageResponse>{`$${tex}$`}</CompactMessageResponse>,
    );

    expect(container.querySelector(".katex")).toBeNull();
    expect(
      container.querySelector("[data-math-fallback=tokens]"),
    ).toHaveTextContent(tex);
  });

  it("keeps KaTeX trust disabled for author-controlled commands", () => {
    const { container } = render(
      <CompactMessageResponse>
        {String.raw`$\href{https://attacker.example}{click}$ $\htmlClass{owned}{x}$`}
      </CompactMessageResponse>,
    );

    expect(container.querySelector("a[href], img, script, style")).toBeNull();
    expect(container.querySelector(".owned")).toBeNull();
    expect(container.querySelectorAll(".katex")).toHaveLength(2);
    expect(container).toHaveTextContent(String.raw`\href`);
    expect(container).toHaveTextContent(String.raw`\htmlClass`);
  });

  it("keeps an edited trailer attached after inline math", () => {
    const { container } = render(
      <CompactMessageResponse trailer={{ text: "(編集済み)" }}>
        {"答えは $x=1$"}
      </CompactMessageResponse>,
    );

    const paragraph = container.querySelector("p");
    expect(paragraph?.querySelector(".katex")).not.toBeNull();
    expect(paragraph?.querySelector("[data-trailer]")).toHaveTextContent(
      "(編集済み)",
    );
  });

  it.each([
    ["$E=mc^2$", "E=mc^2"],
    [String.raw`$$\frac{1}{2}$$`, String.raw`\frac{1}{2}`],
  ])("copies one TeX source instead of duplicate KaTeX text: %s", (source, tex) => {
    const { container } = render(
      <CompactMessageResponse>{source}</CompactMessageResponse>,
    );
    const boundary = container.querySelector("[data-compact-message-response]");
    const math = container.querySelector(
      "[data-math-inline], [data-math-display]",
    );
    const selection = window.getSelection();
    const range = document.createRange();
    if (!boundary || !math || !selection)
      throw new Error("math copy fixture missing");
    range.selectNodeContents(math);
    selection.removeAllRanges();
    selection.addRange(range);
    const setData = vi.fn();

    fireEvent.copy(boundary, { clipboardData: { setData } });

    expect(setData).toHaveBeenCalledWith("text/plain", tex);
  });
});
