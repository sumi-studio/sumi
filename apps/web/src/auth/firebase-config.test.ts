import { describe, expect, it } from "vitest";
import { resolveFirebaseConfiguration } from "./firebase-config";

describe("Sumi Studio Firebase web configuration", () => {
  it("uses the public Sumi Studio client only as a local-development fallback", () => {
    expect(resolveFirebaseConfiguration({}, true)).toEqual({
      configured: true,
      config: {
        apiKey: "AIzaSyCDvzBtM6YFgjLVRh9l2OeZzDqy2QlKoy0",
        authDomain: "sumi-studio.firebaseapp.com",
        projectId: "sumi-studio",
        storageBucket: "sumi-studio.firebasestorage.app",
        messagingSenderId: "393597537629",
        appId: "1:393597537629:web:a3ce178f79d93f238bacb4",
        measurementId: "G-9S2XL0H4FD",
      },
    });
  });

  it("leaves an unconfigured production build fail-closed", () => {
    expect(resolveFirebaseConfiguration({}, false)).toEqual({
      configured: false,
      config: {
        apiKey: undefined,
        authDomain: undefined,
        projectId: undefined,
        appId: undefined,
      },
    });
  });

  it("does not combine a partial deployment config with another project", () => {
    expect(
      resolveFirebaseConfiguration(
        { VITE_FIREBASE_PROJECT_ID: "another-project" },
        true,
      ),
    ).toEqual({
      configured: false,
      config: {
        apiKey: undefined,
        authDomain: undefined,
        projectId: "another-project",
        appId: undefined,
      },
    });
  });

  it("accepts a complete explicit deployment without Sumi Studio fallbacks", () => {
    expect(
      resolveFirebaseConfiguration(
        {
          VITE_FIREBASE_API_KEY: "other-key",
          VITE_FIREBASE_AUTH_DOMAIN: "other.example",
          VITE_FIREBASE_PROJECT_ID: "other-project",
          VITE_FIREBASE_APP_ID: "other-app",
        },
        false,
      ),
    ).toEqual({
      configured: true,
      config: {
        apiKey: "other-key",
        authDomain: "other.example",
        projectId: "other-project",
        appId: "other-app",
        storageBucket: undefined,
        messagingSenderId: undefined,
        measurementId: undefined,
      },
    });
  });
});
