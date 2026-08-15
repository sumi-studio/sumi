import "@sumi/ui/globals.css";
import { CompactMessageResponse } from "@sumi/ui/ai-elements/compact-message-response";
import { createRoot } from "react-dom/client";

const macroExpansionProbe = `\\def\\boom{${"x".repeat(200)}}${"\\boom".repeat(200)}`;
const aggregateMathProbe = Array.from(
  { length: 4_000 },
  () => String.raw`$\frac{1}{2}$`,
).join(" ");

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
      <section id="copy-escaped-tex">
        <CompactMessageResponse>
          {String.raw`literal $a\_b$`}
        </CompactMessageResponse>
      </section>
      <section id="currency-adjacent">
        <CompactMessageResponse>
          {"Price $5, formula:$x$"}
        </CompactMessageResponse>
      </section>
      <section id="currency-japanese">
        <CompactMessageResponse>
          {"価格は$5/個、式は$x$"}
        </CompactMessageResponse>
      </section>
      <section id="numeric-formula-adjacent">
        <CompactMessageResponse>
          {"結果は$5 + x$です。次は$y$"}
        </CompactMessageResponse>
      </section>
      <section id="macro-expansion">
        <CompactMessageResponse>{`$${macroExpansionProbe}$`}</CompactMessageResponse>
      </section>
      <section id="aggregate-math">
        <CompactMessageResponse>{aggregateMathProbe}</CompactMessageResponse>
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
