export class ApiError extends Error {
  status: number;

  detail: unknown;

  constructor(message: string, status: number, detail: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
  }
}

function normalizeApiPath(path: string): string {
  const trimmed = path.trim();
  if (!trimmed) {
    return '/api';
  }
  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
    return trimmed;
  }
  if (trimmed.startsWith('/api/')) {
    return trimmed;
  }
  if (trimmed === '/api') {
    return trimmed;
  }
  if (trimmed.startsWith('api/')) {
    return `/${trimmed}`;
  }
  return `/api${trimmed.startsWith('/') ? trimmed : `/${trimmed}`}`;
}

function withQuery(path: string, params?: Record<string, string | number | boolean | null | undefined>): string {
  if (!params || Object.keys(params).length === 0) {
    return path;
  }
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === '') {
      continue;
    }
    search.set(key, String(value));
  }
  const query = search.toString();
  if (!query) {
    return path;
  }
  return `${path}${path.includes('?') ? '&' : '?'}${query}`;
}

async function parseResponse<T>(response: Response): Promise<T> {
  const contentType = response.headers.get('content-type') || '';
  const isJson = contentType.includes('application/json');

  let payload: unknown = null;
  if (isJson) {
    payload = await response.json();
  } else {
    const text = await response.text();
    payload = text || null;
  }

  if (!response.ok) {
    const detail =
      typeof payload === 'object' && payload !== null && 'detail' in (payload as Record<string, unknown>)
        ? (payload as Record<string, unknown>).detail
        : payload;
    throw new ApiError(
      `Request failed with status ${response.status}`,
      response.status,
      detail,
    );
  }

  return payload as T;
}

async function request<T>(
  method: 'GET' | 'POST' | 'PUT' | 'DELETE',
  path: string,
  options?: {
    body?: unknown;
    params?: Record<string, string | number | boolean | null | undefined>;
    signal?: AbortSignal;
  },
): Promise<T> {
  const endpoint = withQuery(normalizeApiPath(path), options?.params);

  const response = await fetch(endpoint, {
    method,
    headers: options?.body !== undefined ? { 'Content-Type': 'application/json' } : undefined,
    body: options?.body !== undefined ? JSON.stringify(options.body) : undefined,
    signal: options?.signal,
  });

  return parseResponse<T>(response);
}

export const api = {
  get<T>(path: string, params?: Record<string, string | number | boolean | null | undefined>, signal?: AbortSignal) {
    return request<T>('GET', path, { params, signal });
  },
  post<T>(path: string, body?: unknown, signal?: AbortSignal) {
    return request<T>('POST', path, { body, signal });
  },
  put<T>(path: string, body?: unknown, signal?: AbortSignal) {
    return request<T>('PUT', path, { body, signal });
  },
  delete<T>(path: string, signal?: AbortSignal) {
    return request<T>('DELETE', path, { signal });
  },
};

export function wsUrl(path: string): string {
  const normalizedPath = normalizeApiPath(path);
  if (normalizedPath.startsWith('ws://') || normalizedPath.startsWith('wss://')) {
    return normalizedPath;
  }
  if (normalizedPath.startsWith('http://') || normalizedPath.startsWith('https://')) {
    return normalizedPath.replace(/^http/, 'ws');
  }
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}${normalizedPath}`;
}
