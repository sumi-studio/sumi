import type { FirebaseOptions } from "firebase/app";

const localDevelopmentDefaults = {
  apiKey: "AIzaSyCDvzBtM6YFgjLVRh9l2OeZzDqy2QlKoy0",
  authDomain: "sumi-studio.firebaseapp.com",
  projectId: "sumi-studio",
  storageBucket: "sumi-studio.firebasestorage.app",
  messagingSenderId: "393597537629",
  appId: "1:393597537629:web:a3ce178f79d93f238bacb4",
  measurementId: "G-9S2XL0H4FD",
} satisfies FirebaseOptions;

interface FirebaseConfigurationEnvironment {
  VITE_FIREBASE_API_KEY?: string;
  VITE_FIREBASE_AUTH_DOMAIN?: string;
  VITE_FIREBASE_PROJECT_ID?: string;
  VITE_FIREBASE_STORAGE_BUCKET?: string;
  VITE_FIREBASE_MESSAGING_SENDER_ID?: string;
  VITE_FIREBASE_APP_ID?: string;
  VITE_FIREBASE_MEASUREMENT_ID?: string;
}

interface ResolvedFirebaseConfiguration {
  config: FirebaseOptions;
  configured: boolean;
}

function trimmed(value: string | undefined): string | undefined {
  return value?.trim() || undefined;
}

/**
 * Firebase's web configuration is public client metadata, not a credential.
 * Even so, a production build must not silently choose a real tenant. The
 * Sumi Studio values are therefore only a local Vite-development fallback.
 */
export function resolveFirebaseConfiguration(
  environment: FirebaseConfigurationEnvironment,
  development: boolean,
): ResolvedFirebaseConfiguration {
  const required = {
    apiKey: trimmed(environment.VITE_FIREBASE_API_KEY),
    authDomain: trimmed(environment.VITE_FIREBASE_AUTH_DOMAIN),
    projectId: trimmed(environment.VITE_FIREBASE_PROJECT_ID),
    appId: trimmed(environment.VITE_FIREBASE_APP_ID),
  };
  const requiredValues = Object.values(required);
  const explicitlyConfigured = requiredValues.every(
    (value): value is string => value !== undefined,
  );
  const hasPartialConfiguration = requiredValues.some(
    (value) => value !== undefined,
  );

  if (explicitlyConfigured) {
    return {
      configured: true,
      config: {
        ...required,
        storageBucket: trimmed(environment.VITE_FIREBASE_STORAGE_BUCKET),
        messagingSenderId: trimmed(
          environment.VITE_FIREBASE_MESSAGING_SENDER_ID,
        ),
        measurementId: trimmed(environment.VITE_FIREBASE_MEASUREMENT_ID),
      },
    };
  }
  if (development && !hasPartialConfiguration) {
    return { configured: true, config: localDevelopmentDefaults };
  }
  return { configured: false, config: required };
}

// Analytics is intentionally not initialized: Authentication does not depend
// on it.
const resolved = resolveFirebaseConfiguration(
  {
    VITE_FIREBASE_API_KEY: import.meta.env.VITE_FIREBASE_API_KEY,
    VITE_FIREBASE_AUTH_DOMAIN: import.meta.env.VITE_FIREBASE_AUTH_DOMAIN,
    VITE_FIREBASE_PROJECT_ID: import.meta.env.VITE_FIREBASE_PROJECT_ID,
    VITE_FIREBASE_STORAGE_BUCKET: import.meta.env.VITE_FIREBASE_STORAGE_BUCKET,
    VITE_FIREBASE_MESSAGING_SENDER_ID: import.meta.env
      .VITE_FIREBASE_MESSAGING_SENDER_ID,
    VITE_FIREBASE_APP_ID: import.meta.env.VITE_FIREBASE_APP_ID,
    VITE_FIREBASE_MEASUREMENT_ID: import.meta.env.VITE_FIREBASE_MEASUREMENT_ID,
  },
  import.meta.env.DEV,
);
export const firebaseConfig = resolved.config;
export const isFirebaseConfigured = resolved.configured;
