import { useState, useEffect, useCallback } from 'react';
import { evaluationsApi } from '../lib/api/evaluations';
import { schoolsApi } from '../lib/api/schools';
import type { EvaluationResponse, TieredSchoolList, SchoolResponse, EvaluationWithSchool } from '../lib/types';

export interface TieredListWithSchools {
  reach: EvaluationWithSchool[];
  target: EvaluationWithSchool[];
  safety: EvaluationWithSchool[];
  likely: EvaluationWithSchool[];
}

export function useEvaluations(studentId: string | null) {
  const [evaluations, setEvaluations] = useState<EvaluationResponse[]>([]);
  const [tieredList, setTieredList] = useState<TieredListWithSchools | null>(null);
  const [schoolCache, setSchoolCache] = useState<Record<string, SchoolResponse>>({});
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const enrichWithSchools = useCallback(
    async (tiers: TieredSchoolList): Promise<TieredListWithSchools> => {
      // Collect unique school IDs
      const allEvals = [...tiers.reach, ...tiers.target, ...tiers.safety, ...tiers.likely];
      const schoolIds = [...new Set(allEvals.map((e) => e.school_id))];

      // Fetch missing schools
      const missing = schoolIds.filter((id) => !schoolCache[id]);
      const newSchools: Record<string, SchoolResponse> = {};
      await Promise.allSettled(
        missing.map(async (id) => {
          try {
            const school = await schoolsApi.get(id);
            newSchools[id] = school;
          } catch {
            // skip if school fetch fails
          }
        }),
      );

      const merged = { ...schoolCache, ...newSchools };
      setSchoolCache(merged);

      const enrich = (list: EvaluationResponse[]): EvaluationWithSchool[] =>
        list.map((ev) => ({ ...ev, school: merged[ev.school_id] }));

      return {
        reach: enrich(tiers.reach),
        target: enrich(tiers.target),
        safety: enrich(tiers.safety),
        likely: enrich(tiers.likely),
      };
    },
    [schoolCache],
  );

  const fetchTiers = useCallback(async () => {
    if (!studentId) return;
    setIsLoading(true);
    setError(null);
    try {
      const tiers = await evaluationsApi.tiers(studentId);
      const allEvals = [...tiers.reach, ...tiers.target, ...tiers.safety, ...tiers.likely];
      setEvaluations(allEvals);
      const enriched = await enrichWithSchools(tiers);
      setTieredList(enriched);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [studentId, enrichWithSchools]);

  useEffect(() => {
    fetchTiers();
  }, [studentId]); // intentionally not including fetchTiers to avoid infinite loop

  const evaluate = useCallback(
    async (schoolId: string) => {
      if (!studentId) return;
      await evaluationsApi.evaluate(studentId, schoolId);
      await fetchTiers();
    },
    [studentId, fetchTiers],
  );

  return { evaluations, tieredList, isLoading, error, evaluate, refetch: fetchTiers };
}
