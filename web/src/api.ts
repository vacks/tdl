export class APIError extends Error {
  constructor(message: string, readonly status: number) {
    super(message)
    this.name = 'APIError'
  }
}

export async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, { credentials: 'include', headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'TDL-Web', ...init?.headers }, ...init })
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new APIError(body.error || '请求失败', response.status)
  return body as T
}
