import { useState, useCallback } from 'react';
import { simulationsApi, WhatIfResponse } from '../lib/api/simulations';

export function useSimulations() {
  const [result, setResult] = useState<WhatIfResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const runWhatIf = useCallback(async (studentId: string, schoolId: string, interventions: Record<string, unknown>) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await simulationsApi.whatIf(studentId, schoolId, interventions);
      setResult(data);
      return data;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  return { result, isLoading, error, runWhatIf };
}
