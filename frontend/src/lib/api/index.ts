type QueryValue = string | number | boolean | null | undefined;
type QueryParams = Record<string, QueryValue>;

function normalizePath(path: string): string {
  if (/^https?:\/\//.test(path)) return path;
  if (path.startsWith('/api/')) return path;
  return `/api${path.startsWith('/') ? path : `/${path}`}`;
}

function withQuery(path: string, query?: QueryParams): string {
  if (!query || Object.keys(query).length === 0) return normalizePath(path);
  const url = new URL(normalizePath(path), window.location.origin);
  Object.entries(query).forEach(([key, value]) => {
    if (value == null || value === '') return;
    url.searchParams.set(key, String(value));
  });
  return `${url.pathname}${url.search}`;
}

async function request<T>(path: string, init?: RequestInit, query?: QueryParams): Promise<T> {
  const response = await fetch(withQuery(path, query), {
    credentials: 'same-origin',
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
    ...init,
  });

  if (!response.ok) {
    let detail = response.statusText;
    try {
      const data = await response.json();
      detail = data?.detail ?? data?.message ?? detail;
    } catch {
      const text = await response.text();
      if (text) detail = text;
    }
    throw new Error(detail || `Request failed: ${response.status}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  const contentType = response.headers.get('content-type') ?? '';
  if (contentType.includes('application/json')) {
    return response.json() as Promise<T>;
  }

  return response.text() as unknown as T;
}

export const api = {
  get<T>(path: string, query?: QueryParams, init?: RequestInit) {
    return request<T>(path, { method: 'GET', ...(init ?? {}) }, query);
  },
  post<T>(path: string, body?: unknown, init?: RequestInit) {
    return request<T>(
      path,
      {
        method: 'POST',
        body: body === undefined ? undefined : JSON.stringify(body),
        ...(init ?? {}),
      },
    );
  },
  put<T>(path: string, body?: unknown, init?: RequestInit) {
    return request<T>(
      path,
      {
        method: 'PUT',
        body: body === undefined ? undefined : JSON.stringify(body),
        ...(init ?? {}),
      },
    );
  },
  patch<T>(path: string, body?: unknown, init?: RequestInit) {
    return request<T>(
      path,
      {
        method: 'PATCH',
        body: body === undefined ? undefined : JSON.stringify(body),
        ...(init ?? {}),
      },
    );
  },
  delete<T>(path: string, init?: RequestInit) {
    return request<T>(path, { method: 'DELETE', ...(init ?? {}) });
  },
};

export function wsUrl(path: string): string {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  return `${protocol}//${window.location.host}${normalizePath(path)}`;
}
