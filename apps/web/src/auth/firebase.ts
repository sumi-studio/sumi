import { type FirebaseApp, getApp, getApps, initializeApp } from "firebase/app";
import { type Auth, connectAuthEmulator, getAuth } from "firebase/auth";
import { firebaseConfig, isFirebaseConfigured } from "./firebase-config";

const appName = "sumi-web";
let appInstance: FirebaseApp | null = null;
let authInstance: Auth | null = null;
let emulatorConnected = false;

export function getFirebaseApp(): FirebaseApp {
  if (!isFirebaseConfigured) {
    throw new Error("Firebase Authentication is not configured.");
  }
  if (!appInstance) {
    appInstance = getApps().some((app) => app.name === appName)
      ? getApp(appName)
      : initializeApp(firebaseConfig, appName);
  }
  return appInstance;
}

export function getFirebaseAuth(): Auth {
  if (!authInstance) {
    authInstance = getAuth(getFirebaseApp());
  }
  const emulatorURL = import.meta.env.VITE_FIREBASE_AUTH_EMULATOR_URL?.trim();
  if (emulatorURL && !emulatorConnected) {
    connectAuthEmulator(authInstance, emulatorURL, {
      disableWarnings: true,
    });
    emulatorConnected = true;
  }
  return authInstance;
}
