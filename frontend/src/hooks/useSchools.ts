import { useState, useCallback } from 'react';
import { schoolsApi } from '../lib/api/schools';
import type { SchoolResponse } from '../lib/types';

export interface SchoolFilters {
  query?: string;
  state?: string;
  min_rank?: string;
  max_rank?: string;
  page?: string;
  per_page?: string;
}

export function useSchools() {
  const [schools, setSchools] = useState<SchoolResponse[]>([]);
  const [total, setTotal] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [filters, setFilters] = useState<SchoolFilters>({});

  const search = useCallback(
    async (params?: SchoolFilters) => {
      const merged = { ...filters, ...params };
      setIsLoading(true);
      setError(null);
      try {
        // Strip undefined/empty values before sending
        const cleanParams: Record<string, string> = {};
        for (const [k, v] of Object.entries(merged)) {
          if (v !== undefined && v !== '') cleanParams[k] = v;
        }
        const result = await schoolsApi.list(cleanParams);
        setSchools(result.items);
        setTotal(result.total);
        if (params) setFilters(merged);
      } catch (err) {
        setError(err instanceof Error ? err : new Error(String(err)));
      } finally {
        setIsLoading(false);
      }
    },
    [filters],
  );

  return { schools, total, isLoading, error, search, filters, setFilters };
}
