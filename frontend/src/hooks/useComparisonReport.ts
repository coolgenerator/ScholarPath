import { useCallback, useState } from 'react';
import { evaluationsApi, type CompareStreamEvent } from '../lib/api/evaluations';
import type {
  CompareReportRequest,
  OrientationComparison,
  OrientationCausalGraph,
} from '../lib/types';

export interface StreamingReport {
  orientations: OrientationComparison[];
  causalGraphs: OrientationCausalGraph[];
  recommendation: string | null;
  confidence: number | null;
  studentId: string | null;
  schoolIds: string[];
}

const EMPTY_REPORT: StreamingReport = {
  orientations: [],
  causalGraphs: [],
  recommendation: null,
  confidence: null,
  studentId: null,
  schoolIds: [],
};

export function useComparisonReport() {
  const [report, setReport] = useState<StreamingReport | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [currentStep, setCurrentStep] = useState<string | null>(null);
  const [error, setError] = useState<Error | null>(null);

  const generate = useCallback(
    async (studentId: string, body: CompareReportRequest) => {
      setIsLoading(true);
      setError(null);
      setReport({ ...EMPTY_REPORT, schoolIds: body.school_ids });
      setCurrentStep(null);

      try {
        await evaluationsApi.compareReportStream(studentId, body, (event: CompareStreamEvent) => {
          if (event.event === 'orientation') {
            const comparison = event.data.comparison as unknown as OrientationComparison;
            const graph = event.data.causal_graph as unknown as OrientationCausalGraph;
            setCurrentStep(comparison.orientation);
            setReport((prev) => {
              if (!prev) return prev;
              return {
                ...prev,
                orientations: [...prev.orientations, comparison],
                causalGraphs: [...prev.causalGraphs, graph],
              };
            });
          } else if (event.event === 'recommendation') {
            setCurrentStep('recommendation');
            setReport((prev) => {
              if (!prev) return prev;
              return {
                ...prev,
                recommendation: event.data.recommendation as string,
                confidence: event.data.confidence as number,
                studentId: event.data.student_id as string,
                schoolIds: event.data.school_ids as string[],
              };
            });
          } else if (event.event === 'error') {
            console.warn('Comparison stream error:', event.data.message);
          }
        });
      } catch (err) {
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
        setCurrentStep(null);
      }
    },
    [],
  );

  const clear = useCallback(() => {
    setReport(null);
    setError(null);
    setCurrentStep(null);
  }, []);

  return { report, isLoading, currentStep, error, generate, clear };
}
