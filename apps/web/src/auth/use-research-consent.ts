import { useCallback, useEffect, useState } from "react";
import {
  AuthAPIError,
  getResearchConsent,
  type ResearchConsentState,
  setResearchConsent,
} from "./session-client";

export interface ResearchConsent {
  loading: boolean;
  decided: boolean;
  granted: boolean;
  error: string | null;
  setConsent: (grant: boolean) => Promise<void>;
  refresh: () => Promise<void>;
}

const undecided: ResearchConsentState = { decided: false, granted: false };

export function useResearchConsent(enabled: boolean): ResearchConsent {
  const [state, setState] = useState<ResearchConsentState>(undecided);
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    if (!enabled) {
      setState(undecided);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const next = await getResearchConsent();
      setState(next);
    } catch (err) {
      setError(consentErrorMessage(err));
      setState(undecided);
    } finally {
      setLoading(false);
    }
  }, [enabled]);

  useEffect(() => {
    if (!enabled) {
      setState(undecided);
      setLoading(false);
      return;
    }
    void refresh();
  }, [enabled, refresh]);

  const setConsent = useCallback(async (grant: boolean) => {
    setLoading(true);
    setError(null);
    try {
      const next = await setResearchConsent(grant);
      setState(next);
    } catch (err) {
      setError(consentErrorMessage(err));
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    loading,
    decided: state.decided,
    granted: state.granted,
    error,
    setConsent,
    refresh,
  };
}

function consentErrorMessage(err: unknown): string {
  if (err instanceof AuthAPIError) {
    return "同意状態の確認に失敗しました。もう一度お試しください。";
  }
  return "同意状態の確認に失敗しました。もう一度お試しください。";
}
