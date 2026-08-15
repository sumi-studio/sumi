(() => {
  const root = document.documentElement;
  let preference = "system";
  try {
    const stored = localStorage.getItem("sumi:theme");
    if (stored === "light" || stored === "dark" || stored === "system") {
      preference = stored;
    }
  } catch {}
  const resolved =
    preference === "system"
      ? matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light"
      : preference;
  root.dataset.theme = resolved;
  root.dataset.themePreference = preference;
})();
