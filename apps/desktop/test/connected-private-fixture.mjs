// Loopback fixtures for the configured browser-connected entry regression
// (apps/api/internal/browsertabs/connected_private_test.go): an owned delivery
// form that echoes the private values it saves — in its control values, page
// text across the 2000/6000-character cuts, title and URL — and the
// contract-checking Jev double. Every Jev request body is written to
// $OUT/jev-requests.json and every save to $OUT/site.json. Values are synthetic.
import { writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { formPolicy, startJevFixture } from "./jev-fixture.mjs";

const out = process.env.OUT;
const saves = [];
const page = `<!doctype html><title>Delivery form</title>
<p id="echo"></p>
<h1>Delivery details</h1>
<form id="f">
<label>Name <input id="name" aria-label="Name"></label>
<label>Address <textarea id="address" aria-label="Address"></textarea></label>
<label>Passphrase <input id="passphrase" aria-label="Passphrase"></label>
<label>Token <input id="token" aria-label="Token"></label>
<label>Member <input id="member" aria-label="Member"></label>
<button>Save</button></form>
<script>
const v = (id) => document.getElementById(id).value;
document.getElementById("f").onsubmit = (event) => {
  event.preventDefault();
  const saved = { name: v("name"), address: v("address"), passphrase: v("passphrase"), token: v("token"), member: v("member") };
  fetch("/save", { method: "POST", body: JSON.stringify(saved) }).then(() => {
    // The member number straddles character 2000 (final_page.text) and
    // character 6000 (the page text sent to Jev); the rest is echoed whole.
    let text = "Saved for " + saved.name + ". ";
    text += "q".repeat(1990 - text.length) + " " + saved.member + " " + saved.address + " " + saved.passphrase + " " + saved.token + " ";
    text += "p".repeat(5991 - text.length) + saved.member;
    document.getElementById("echo").textContent = text;
    document.title = "Saved " + saved.token;
    history.replaceState(null, "", "/?" + new URLSearchParams({ address: saved.address, pass: saved.passphrase }));
  });
};
</script>`;
const site = createServer(async (req, res) => {
  if (req.url === "/save") {
    let raw = "";
    for await (const chunk of req) raw += chunk;
    saves.push(JSON.parse(raw));
    writeFileSync(`${out}/site.json`, JSON.stringify(saves, null, 1));
    res.end("{}");
    return;
  }
  res.setHeader("Content-Type", "text/html");
  res.end(page);
});
await new Promise((r) =>
  site.listen(Number(process.env.SITE_PORT), "127.0.0.1", r),
);
const policy = formPolicy({ doneText: "Saved for" });
const jev = await startJevFixture({
  port: Number(process.env.JEV_PORT),
  policy: (body, n) => {
    writeFileSync(
      `${out}/jev-requests.json`,
      JSON.stringify(
        jev.requests.map((r) => r.raw),
        null,
        1,
      ),
    );
    return policy(body, n);
  },
});
console.log("fixtures ready");
