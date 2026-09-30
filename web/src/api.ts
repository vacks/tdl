export class APIError extends Error {
  constructor(message: string, readonly status: number) {
    super(message)
    this.name = 'APIError'
  }
}

// A request that never answers must not leave the page spinning forever.
// fetch has no timeout of its own, so a stalled connection - or a server that
// is still working - would hold the button in its loading state indefinitely.
// The server bounds its own work; this is the client-side backstop.
const REQUEST_TIMEOUT_MS = 30_000

export async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController()
  const timer = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  let response: Response
  try {
    response = await fetch(url, { credentials: 'include', headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'TDL-Web', ...init?.headers }, ...init, signal: init?.signal ?? controller.signal })
  } catch (error) {
    if (controller.signal.aborted) throw new APIError('请求超时，请稍后重试', 0)
    throw error
  } finally {
    window.clearTimeout(timer)
  }
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new APIError(body.error || '请求失败', response.status)
  return body as T
}
