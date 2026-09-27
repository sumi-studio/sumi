import { useEffect, useState } from "react";
import { createCloudBrowserAPI } from "./api";

const cache = new Map<string, Promise<boolean>>();

/** Whether this deployment offers the Cloud browser to the signed-in person
 * (one request per account and page load). */
export function useCloudBrowserAvailable(accountId: string | null): boolean {
  const [available, setAvailable] = useState(false);
  useEffect(() => {
    if (!accountId) {
      setAvailable(false);
      return;
    }
    let current = true;
    let pending = cache.get(accountId);
    if (!pending) {
      pending = createCloudBrowserAPI()
        .overview()
        .then((o) => o.configured)
        .catch(() => {
          cache.delete(accountId);
          return false;
        });
      cache.set(accountId, pending);
    }
    void pending.then((value) => {
      if (current) setAvailable(value);
    });
    return () => {
      current = false;
    };
  }, [accountId]);
  return available;
}
