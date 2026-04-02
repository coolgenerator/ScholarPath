import { api } from './index';
import type {
  OfferComparisonResponse,
  OfferCreate,
  OfferResponse as OfferResponseModel,
  OfferUpdate,
} from '../types';

export type OfferResponse = OfferResponseModel;
export type OfferComparison = OfferComparisonResponse;

export const offersApi = {
  list(studentId: string) {
    return api.get<OfferResponse[]>(`/offers/students/${studentId}/offers`);
  },
  create(studentId: string, payload: OfferCreate) {
    return api.post<OfferResponse>(`/offers/students/${studentId}/offers`, payload);
  },
  update(_studentId: string, offerId: string, payload: OfferUpdate) {
    return api.put<OfferResponse>(`/offers/${offerId}`, payload);
  },
  compare(studentId: string) {
    return api.get<OfferComparison>(`/offers/students/${studentId}/offers/compare`);
  },
};
