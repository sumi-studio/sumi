import "@sumi/ui/globals.css";
import { CompactMessageResponse } from "@sumi/ui/ai-elements/compact-message-response";
import { createRoot } from "react-dom/client";

function App() {
  return (
    <main className="space-y-6 p-4">
      <section id="copy-energy">
        <CompactMessageResponse>{"$E=mc^2$"}</CompactMessageResponse>
      </section>
      <section id="copy-fraction">
        <CompactMessageResponse>
          {String.raw`$$\frac{1}{2}$$`}
        </CompactMessageResponse>
      </section>
      <section id="copy-mixed">
        <CompactMessageResponse>
          {"Start $E=mc^2$ tail\nnext line\n\nSecond $\\frac{1}{2}$ end"}
        </CompactMessageResponse>
      </section>
      <section id="narrow-message" className="w-[220px] border p-2">
        <CompactMessageResponse>
          {
            "$\\displaystyle \\sum_{i=1}^{100} \\frac{i^4 + 3i^3 + 7i^2 + 11i + 13}{i^2 + 1}$"
          }
        </CompactMessageResponse>
      </section>
    </main>
  );
}

const root = document.getElementById("root");
if (!root) throw new Error("compact math harness root missing");
createRoot(root).render(<App />);
(window as unknown as Record<string, unknown>).__compactMathReady = true;
