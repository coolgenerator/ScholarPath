import { useState, useCallback } from 'react';
import { simulationsApi, ScenarioCompareResponse, WhatIfResponse } from '../lib/api/simulations';

export function useSimulations() {
  const [result, setResult] = useState<WhatIfResponse | null>(null);
  const [comparisonResult, setComparisonResult] = useState<ScenarioCompareResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const runWhatIf = useCallback(async (studentId: string, schoolId: string, interventions: Record<string, unknown>) => {
    setIsLoading(true);
    setError(null);
    setResult(null);
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

  const compareScenarios = useCallback(async (
    studentId: string,
    scenarios: Array<{ interventions: Record<string, unknown> }>,
  ) => {
    setIsLoading(true);
    setError(null);
    setComparisonResult(null);
    try {
      const data = await simulationsApi.compareScenarios(studentId, scenarios);
      setComparisonResult(data);
      return data;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  const clearComparison = useCallback(() => {
    setComparisonResult(null);
    setError(null);
  }, []);

  return { result, comparisonResult, isLoading, error, runWhatIf, compareScenarios, clearComparison };
}
