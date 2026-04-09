import { api } from './index';

export interface OtpRequestResponse {
  message: string;
}

export interface OtpVerifyResponse {
  access_token: string;
  token_type: string;
  user_id: string;
  student_id: string | null;
}

export interface MeResponse {
  user_id: string;
  email: string;
  student_id: string | null;
  is_active: boolean;
}

export const authApi = {
  requestOtp(email: string) {
    return api.post<OtpRequestResponse>('/auth/otp/request', { email });
  },
  verifyOtp(email: string, code: string) {
    return api.post<OtpVerifyResponse>('/auth/otp/verify', { email, code });
  },
  getMe() {
    return api.get<MeResponse>('/auth/me');
  },
};
