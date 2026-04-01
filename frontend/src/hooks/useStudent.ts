import { useState, useCallback } from 'react';
import { studentsApi } from '../lib/api/students';
import type { StudentCreate, StudentResponse } from '../lib/types';

export function useStudent() {
  const [student, setStudent] = useState<StudentResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchStudent = useCallback(async (id: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const data = await studentsApi.get(id);
      setStudent(data);
      return data;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  const createStudent = useCallback(async (data: StudentCreate) => {
    setIsLoading(true);
    setError(null);
    try {
      const created = await studentsApi.create(data);
      setStudent(created);
      return created;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      setIsLoading(false);
    }
  }, []);

  return { student, isLoading, error, fetchStudent, createStudent };
}
