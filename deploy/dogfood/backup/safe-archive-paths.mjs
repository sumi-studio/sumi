let input = "";
for await (const chunk of process.stdin) input += chunk;
for (const raw of input.split("\n")) {
  if (!raw) continue;
  if (raw === "." || raw === "./") continue;
  const path = raw.startsWith("./") ? raw.slice(2) : raw;
  const parts = path.endsWith("/")
    ? path.slice(0, -1).split("/")
    : path.split("/");
  if (
    !path ||
    path.startsWith("/") ||
    parts.some((part) => part === "" || part === "." || part === "..") ||
    path.includes("\\") ||
    [...path].some((character) => {
      const code = character.codePointAt(0) ?? 0;
      return code < 0x20 || code === 0x7f;
    })
  ) {
    throw new Error(`unsafe archive path: ${JSON.stringify(raw)}`);
  }
}
