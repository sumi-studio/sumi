import type { FirebaseOptions } from "firebase/app";

const defaults = {
  apiKey: "AIzaSyCDvzBtM6YFgjLVRh9l2OeZzDqy2QlKoy0",
  authDomain: "sumi-studio.firebaseapp.com",
  projectId: "sumi-studio",
  storageBucket: "sumi-studio.firebasestorage.app",
  messagingSenderId: "393597537629",
  appId: "1:393597537629:web:a3ce178f79d93f238bacb4",
  measurementId: "G-9S2XL0H4FD",
} satisfies FirebaseOptions;

function configuredValue(value: string | undefined, fallback: string): string {
  return value?.trim() || fallback;
}

// Firebase's web configuration identifies the public client; it is not a
// credential. Deployments can override every field. Analytics is intentionally
// not initialized: Authentication does not depend on it.
export const firebaseConfig = {
  apiKey: configuredValue(
    import.meta.env.VITE_FIREBASE_API_KEY,
    defaults.apiKey,
  ),
  authDomain: configuredValue(
    import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
    defaults.authDomain,
  ),
  projectId: configuredValue(
    import.meta.env.VITE_FIREBASE_PROJECT_ID,
    defaults.projectId,
  ),
  storageBucket: configuredValue(
    import.meta.env.VITE_FIREBASE_STORAGE_BUCKET,
    defaults.storageBucket,
  ),
  messagingSenderId: configuredValue(
    import.meta.env.VITE_FIREBASE_MESSAGING_SENDER_ID,
    defaults.messagingSenderId,
  ),
  appId: configuredValue(import.meta.env.VITE_FIREBASE_APP_ID, defaults.appId),
  measurementId: configuredValue(
    import.meta.env.VITE_FIREBASE_MEASUREMENT_ID,
    defaults.measurementId,
  ),
} satisfies FirebaseOptions;

export const isFirebaseConfigured = Boolean(
  firebaseConfig.apiKey &&
    firebaseConfig.authDomain &&
    firebaseConfig.projectId &&
    firebaseConfig.appId,
);
