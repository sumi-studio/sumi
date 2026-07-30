import { describe, expect, it } from "vitest";
import { firebaseConfig, isFirebaseConfigured } from "./firebase-config";

describe("Sumi Studio Firebase web configuration", () => {
  it("has a usable public default without making Analytics part of auth", () => {
    const hasBuildOverride = [
      import.meta.env.VITE_FIREBASE_API_KEY,
      import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
      import.meta.env.VITE_FIREBASE_PROJECT_ID,
      import.meta.env.VITE_FIREBASE_APP_ID,
    ].some((value) => Boolean(value?.trim()));
    if (!hasBuildOverride) {
      expect(firebaseConfig).toMatchObject({
        apiKey: "AIzaSyCDvzBtM6YFgjLVRh9l2OeZzDqy2QlKoy0",
        authDomain: "sumi-studio.firebaseapp.com",
        projectId: "sumi-studio",
        storageBucket: "sumi-studio.firebasestorage.app",
        messagingSenderId: "393597537629",
        appId: "1:393597537629:web:a3ce178f79d93f238bacb4",
        measurementId: "G-9S2XL0H4FD",
      });
    }
    expect(isFirebaseConfigured).toBe(true);
  });
});
