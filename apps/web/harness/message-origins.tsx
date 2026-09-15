import "@sumi/ui/globals.css";
import {
  Attachment,
  AttachmentInfo,
  AttachmentPreview,
  Attachments,
} from "@sumi/ui/ai-elements/attachments";
import { CompactMessageResponse } from "@sumi/ui/ai-elements/compact-message-response";
import {
  MessageResponse,
  type MessageResponseProps,
} from "@sumi/ui/ai-elements/message";
import { useState } from "react";
import { createRoot } from "react-dom/client";

declare global {
  interface Window {
    __originsReady?: boolean;
    __streamAppend?: (chunk: string) => void;
  }
}

const beacon =
  new URLSearchParams(window.location.search).get("beacon") ??
  "http://127.0.0.1:1";

const attackMarkdown = [
  `[docs](https://example.com/docs)`,
  ``,
  `![pixel](${beacon}/markdown-image.png)`,
  ``,
  `<img src="${beacon}/raw-html-image.png" alt="raw" onerror="fetch('${beacon}/onerror-handler')">`,
  ``,
  `<script>fetch("${beacon}/script-tag")</script>`,
  ``,
  `[weak](javascript:fetch("${beacon}/javascript-link"))`,
].join("\n");

// キャスト経由で危険propを押し込む呼び出し側の試み。型を抜けて実行時に
// 渡っても、wrapperが名前で明示転送するだけなのでStreamdownへは届かない。
const overrideAttempt = {
  mode: "streaming",
  isAnimating: true,
  children: [
    `![override-md](${beacon}/override-md.png)`,
    ``,
    `<iframe src="${beacon}/via-allowed-tags" srcdoc="<p>x</p>"></iframe>`,
  ].join("\n"),
  allowedTags: { iframe: ["src", "srcdoc"] },
  literalTagContent: ["iframe"],
  components: {
    img: ({ src }: { src?: string }) => <img src={src} alt="caller" />,
  },
  plugins: {
    math: {
      name: "katex",
      type: "math",
      remarkPlugin: () => {},
      rehypePlugin: () => (tree: { children?: unknown[] }) => {
        tree.children?.push({
          type: "element",
          tagName: "iframe",
          properties: { src: `${beacon}/via-plugins` },
          children: [],
        });
      },
    },
  },
  BlockComponent: () => (
    <img src={`${beacon}/via-block-component`} alt="block" />
  ),
  parseMarkdownIntoBlocksFn: (markdown: string) => [markdown],
  rehypePlugins: [],
  urlTransform: (url: string) => url,
} as unknown as MessageResponseProps;

function App() {
  const [streamed, setStreamed] = useState("stream-head");
  window.__streamAppend = (chunk: string) =>
    setStreamed((current) => current + chunk);
  window.__originsReady = true;

  return (
    <main className="space-y-8 p-4">
      <section id="human-message">
        <CompactMessageResponse>{attackMarkdown}</CompactMessageResponse>
      </section>
      <section id="agent-static">
        <MessageResponse mode="static">{attackMarkdown}</MessageResponse>
      </section>
      <section id="agent-streaming">
        <MessageResponse mode="streaming" isAnimating>
          {streamed}
        </MessageResponse>
      </section>
      <section id="agent-override">
        <MessageResponse {...overrideAttempt} />
      </section>
      <section id="attachment">
        <Attachments variant="grid">
          <Attachment
            data={{
              id: "att-1",
              filename: "ok.png",
              mediaType: "image/png",
              url: `${beacon}/attachment.png`,
            }}
          >
            <AttachmentPreview />
            <AttachmentInfo />
          </Attachment>
        </Attachments>
      </section>
    </main>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("message origins harness root missing");
createRoot(root).render(<App />);
