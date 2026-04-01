import { useState, useCallback, useEffect } from 'react';
import { offersApi, OfferResponse, OfferComparison } from '../lib/api/offers';

export function useOffers(studentId: string | null) {
  const [offers, setOffers] = useState<OfferResponse[]>([]);
  const [comparison, setComparison] = useState<OfferComparison | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  const fetchOffers = useCallback(async () => {
    if (!studentId) return;
    setIsLoading(true);
    setError(null);
    try {
      const data = await offersApi.list(studentId);
      setOffers(data);
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
    } finally {
      setIsLoading(false);
    }
  }, [studentId]);

  useEffect(() => {
    fetchOffers();
  }, [studentId]);

  const createOffer = useCallback(async (data: Parameters<typeof offersApi.create>[1]) => {
    if (!studentId) return null;
    const offer = await offersApi.create(studentId, data);
    await fetchOffers();
    return offer;
  }, [studentId, fetchOffers]);

  const updateOffer = useCallback(async (offerId: string, data: Parameters<typeof offersApi.update>[2]) => {
    if (!studentId) return null;
    const offer = await offersApi.update(studentId, offerId, data);
    await fetchOffers();
    return offer;
  }, [studentId, fetchOffers]);

  const compareOffers = useCallback(async () => {
    if (!studentId) return null;
    try {
      const data = await offersApi.compare(studentId);
      setComparison(data);
      return data;
    } catch (err) {
      setError(err instanceof Error ? err : new Error(String(err)));
      return null;
    }
  }, [studentId]);

  return { offers, comparison, isLoading, error, createOffer, updateOffer, compareOffers, refetch: fetchOffers };
}
