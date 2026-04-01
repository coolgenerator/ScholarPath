import { useState, useCallback } from 'react';
import { reportsApi, GoNoGoReport } from '../lib/api/reports';

export function useReports() {
  const [report, setReport] = useState<GoNoGoReport | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const generateReport = useCallback(async (studentId: string, offerId: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await reportsApi.generateGoNoGo(studentId, offerId);
      setReport(data);
      return data;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  return { report, isLoading, error, generateReport };
}
