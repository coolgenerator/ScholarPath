import { api } from './index';
import type { OfferCreate, OfferResponse, OfferUpdate } from '../types';

export interface OfferComparison {
  offers: OfferResponse[];
  comparison_scores: Array<Record<string, unknown>>;
}

export const offersApi = {
  list(studentId: string) {
    return api.get<OfferResponse[]>(`/offers/students/${studentId}/offers`);
  },
  create(studentId: string, payload: OfferCreate) {
    return api.post<OfferResponse>(`/offers/students/${studentId}/offers`, payload);
  },
  update(studentId: string, offerId: string, payload: OfferUpdate) {
    return api.put<OfferResponse>(`/offers/offers/${offerId}`, payload, {
      headers: {
        'X-Student-Id': studentId,
      },
    });
  },
  compare(studentId: string) {
    return api.get<OfferComparison>(`/offers/students/${studentId}/offers/compare`);
  },
};

export type { OfferCreate, OfferResponse, OfferUpdate } from '../types';
