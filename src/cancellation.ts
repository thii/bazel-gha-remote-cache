export class ClientDisconnectError extends Error {
  constructor() {
    super('cache client disconnected')
    this.name = 'ClientDisconnectError'
  }
}

export class ServerShutdownError extends Error {
  constructor() {
    super('cache server forced request shutdown')
    this.name = 'ServerShutdownError'
  }
}

function errorValue(error: unknown): {
  cause?: unknown
  code?: unknown
  conflict?: unknown
  message?: unknown
  name?: unknown
  rateLimited?: unknown
  statusCode?: unknown
} {
  return typeof error === 'object' && error !== null ? error : {}
}

/**
 * Returns true only when the failing operation was itself cancelled. A real
 * backend response remains observable even if it races with an abort.
 */
export function isAbortDerivedError(
  error: unknown,
  signal?: AbortSignal
): boolean {
  if (signal?.aborted !== true) return false
  const value = errorValue(error)
  if (
    typeof value.statusCode === 'number' ||
    value.rateLimited === true ||
    value.conflict === true
  ) {
    return false
  }
  if (error === signal.reason) return true
  if (value.code === 'ABORT_ERR' || value.name === 'AbortError') return true
  if (
    typeof value.message === 'string' &&
    /\b(?:abort(?:ed)?|cancell?ed)\b/i.test(value.message)
  ) {
    return true
  }
  return value.cause !== undefined && value.cause !== error
    ? isAbortDerivedError(value.cause, signal)
    : false
}

export function isClientDisconnectSignal(signal: AbortSignal): boolean {
  return signal.aborted && signal.reason instanceof ClientDisconnectError
}

export function isClientDisconnectCancellation(
  error: unknown,
  signal: AbortSignal
): boolean {
  return isClientDisconnectSignal(signal) && isAbortDerivedError(error, signal)
}

/** Internal single-flight cancellations are suppressible; shutdowns are not. */
export function isSuppressibleAbortError(
  error: unknown,
  signal?: AbortSignal
): boolean {
  return (
    signal?.aborted === true &&
    !(signal.reason instanceof ServerShutdownError) &&
    isAbortDerivedError(error, signal)
  )
}

export function signalReason(signal: AbortSignal, fallback: string): Error {
  return signal.reason instanceof Error ? signal.reason : new Error(fallback)
}
