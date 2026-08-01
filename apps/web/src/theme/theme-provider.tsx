import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

export type ThemePreference = "system" | "light" | "dark";
type ResolvedTheme = Exclude<ThemePreference, "system">;

const STORAGE_KEY = "sumi:theme";
const SYSTEM_THEME_QUERY = "(prefers-color-scheme: dark)";

interface ThemeContextValue {
  theme: ThemePreference;
  setTheme: (theme: ThemePreference) => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

let releaseFrame: number | undefined;

function isThemePreference(value: unknown): value is ThemePreference {
  return value === "system" || value === "light" || value === "dark";
}

function getSystemTheme(): ResolvedTheme {
  return window.matchMedia(SYSTEM_THEME_QUERY).matches ? "dark" : "light";
}

function resolveTheme(theme: ThemePreference): ResolvedTheme {
  return theme === "system" ? getSystemTheme() : theme;
}

function readThemePreference(): ThemePreference {
  const bootPreference = document.documentElement.dataset.themePreference;
  if (isThemePreference(bootPreference)) {
    return bootPreference;
  }

  try {
    const storedPreference = localStorage.getItem(STORAGE_KEY);
    return isThemePreference(storedPreference) ? storedPreference : "system";
  } catch {
    return "system";
  }
}

function releaseTransitionLock(root: HTMLElement) {
  if (releaseFrame !== undefined) {
    window.cancelAnimationFrame(releaseFrame);
  }

  releaseFrame = window.requestAnimationFrame(() => {
    releaseFrame = window.requestAnimationFrame(() => {
      delete root.dataset.themeChanging;
      releaseFrame = undefined;
    });
  });
}

function applyTheme(
  theme: ThemePreference,
  {
    persist,
    suppressTransitions,
  }: { persist: boolean; suppressTransitions: boolean },
) {
  const root = document.documentElement;

  if (suppressTransitions) {
    root.dataset.themeChanging = "";
    // Ensure the transition lock is active before changing theme variables.
    window.getComputedStyle(root).color;
  }

  root.dataset.theme = resolveTheme(theme);
  root.dataset.themePreference = theme;

  if (persist) {
    try {
      localStorage.setItem(STORAGE_KEY, theme);
    } catch {
      // The selected theme still applies when storage is unavailable.
    }
  }

  if (suppressTransitions) {
    releaseTransitionLock(root);
  }
}

export function initializeTheme() {
  applyTheme(readThemePreference(), {
    persist: false,
    suppressTransitions: false,
  });
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<ThemePreference>(readThemePreference);

  const setTheme = useCallback((nextTheme: ThemePreference) => {
    applyTheme(nextTheme, { persist: true, suppressTransitions: true });
    setThemeState(nextTheme);
  }, []);

  useEffect(() => {
    const mediaQuery = window.matchMedia(SYSTEM_THEME_QUERY);
    const handleSystemThemeChange = () => {
      if (theme === "system") {
        applyTheme("system", {
          persist: false,
          suppressTransitions: true,
        });
      }
    };
    const handleStorageChange = (event: StorageEvent) => {
      if (event.key !== STORAGE_KEY || !isThemePreference(event.newValue)) {
        return;
      }
      applyTheme(event.newValue, {
        persist: false,
        suppressTransitions: true,
      });
      setThemeState(event.newValue);
    };

    mediaQuery.addEventListener("change", handleSystemThemeChange);
    window.addEventListener("storage", handleStorageChange);
    return () => {
      mediaQuery.removeEventListener("change", handleSystemThemeChange);
      window.removeEventListener("storage", handleStorageChange);
    };
  }, [theme]);

  const value = useMemo(() => ({ theme, setTheme }), [setTheme, theme]);

  return (
    <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
  );
}

export function useTheme() {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error("useTheme must be used within ThemeProvider");
  }
  return context;
}
