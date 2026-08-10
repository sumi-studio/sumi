import { ApiClientError } from "./api";

export interface BrowserPushSubscription {
  toJSON(): PushSubscriptionJSON;
  unsubscribe(): Promise<boolean>;
}

export interface BrowserPushManager {
  getSubscription(): Promise<BrowserPushSubscription | null>;
  subscribe(
    options: PushSubscriptionOptionsInit,
  ): Promise<BrowserPushSubscription>;
}

interface RegisterPushOptions {
  pushManager: BrowserPushManager;
  applicationServerKey: BufferSource;
  register: (subscription: PushSubscriptionJSON) => Promise<void>;
  replaceExisting: boolean;
}

function isExpiredSubscription(error: unknown): boolean {
  return (
    error instanceof ApiClientError && error.code === "expired_subscription"
  );
}

async function createSubscription(
  pushManager: BrowserPushManager,
  applicationServerKey: BufferSource,
): Promise<BrowserPushSubscription> {
  return pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey,
  });
}

export async function registerPushSubscription({
  pushManager,
  applicationServerKey,
  register,
  replaceExisting,
}: RegisterPushOptions): Promise<BrowserPushSubscription> {
  let subscription = await pushManager.getSubscription();
  if (subscription && replaceExisting) {
    await subscription.unsubscribe();
    subscription = null;
  }
  subscription ??= await createSubscription(pushManager, applicationServerKey);

  try {
    await register(subscription.toJSON());
    return subscription;
  } catch (error: unknown) {
    if (!isExpiredSubscription(error)) throw error;
    await subscription.unsubscribe();
    const renewed = await createSubscription(pushManager, applicationServerKey);
    await register(renewed.toJSON());
    return renewed;
  }
}
