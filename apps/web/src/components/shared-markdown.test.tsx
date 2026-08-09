// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import {
  CompactMessageResponse,
  type CompactMessageResponseProps,
} from "@sumi/ui/ai-elements/compact-message-response";
import { cleanup, render } from "@testing-library/react";
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
});
