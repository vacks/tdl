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
  try {
    const response = await fetch(url, {
      ...init,
      credentials: 'include',
      // The caller's headers merge into the defaults instead of replacing them.
      // Spreading init after this object let any call that passed headers drop
      // X-Requested-With, which the server requires on every state-changing
      // request - so the write would have been answered with a 403.
      headers: { 'Content-Type': 'application/json', 'X-Requested-With': 'TDL-Web', ...init?.headers },
      signal: init?.signal ?? controller.signal,
    })
    // The body is read inside the timeout window. Clearing the timer as soon as
    // fetch resolved - which is when the response headers arrive, not when the
    // body does - left a server that answered the headers and then stalled
    // holding the page forever, with the one guard that exists for it disarmed.
    const body = await response.json().catch(() => ({}))
    if (!response.ok) throw new APIError((body as { error?: string }).error || '请求失败', response.status)
    return body as T
  } catch (error) {
    // An abort raised by our own timer is a timeout, not a caller cancellation.
    if (controller.signal.aborted) throw new APIError('请求超时，请稍后重试', 0)
    throw error
  } finally {
    window.clearTimeout(timer)
  }
}
