// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { initializeTheme, ThemeProvider, useTheme } from "./theme-provider";

let systemDark = false;
let systemThemeListeners: Array<() => void> = [];

function ThemeControl() {
  const { theme, setTheme } = useTheme();
  return (
    <>
      <output>{theme}</output>
      <button type="button" onClick={() => setTheme("dark")}>
        dark
      </button>
    </>
  );
}

beforeEach(() => {
  systemDark = false;
  systemThemeListeners = [];
  localStorage.clear();
  delete document.documentElement.dataset.theme;
  delete document.documentElement.dataset.themePreference;
  delete document.documentElement.dataset.themeChanging;

  vi.stubGlobal(
    "matchMedia",
    vi.fn().mockImplementation(() => ({
      get matches() {
        return systemDark;
      },
      media: "(prefers-color-scheme: dark)",
      onchange: null,
      addEventListener: (_type: string, listener: () => void) => {
        systemThemeListeners.push(listener);
      },
      removeEventListener: (_type: string, listener: () => void) => {
        systemThemeListeners = systemThemeListeners.filter(
          (candidate) => candidate !== listener,
        );
      },
      addListener: vi.fn(),
      removeListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  );
  vi.stubGlobal(
    "requestAnimationFrame",
    vi.fn((callback: FrameRequestCallback) => {
      callback(performance.now());
      return 1;
    }),
  );
  vi.stubGlobal("cancelAnimationFrame", vi.fn());
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("theme lifecycle", () => {
  it("initializes the resolved system theme before React renders", () => {
    systemDark = true;

    initializeTheme();

    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(document.documentElement.dataset.themePreference).toBe("system");
  });

  it("applies and persists an explicit theme synchronously", () => {
    initializeTheme();
    render(
      <ThemeProvider>
        <ThemeControl />
      </ThemeProvider>,
    );

    fireEvent.click(screen.getByRole("button", { name: "dark" }));

    expect(screen.getByRole("status")).toHaveTextContent("dark");
    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(document.documentElement.dataset.themePreference).toBe("dark");
    expect(localStorage.getItem("sumi:theme")).toBe("dark");
  });

  it("tracks operating-system changes while system theme is selected", () => {
    initializeTheme();
    render(
      <ThemeProvider>
        <ThemeControl />
      </ThemeProvider>,
    );

    systemDark = true;
    for (const listener of systemThemeListeners) {
      listener();
    }

    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(screen.getByRole("status")).toHaveTextContent("system");
  });
});
