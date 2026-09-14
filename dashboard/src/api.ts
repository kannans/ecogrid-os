import type {
  AdviceOut,
  HealthOut,
  OptimizeRunResponse,
  OrchestratorRunResponse,
  PlantOut,
  SchedulePlanOut,
  TelemetryOut,
  WindowStats,
} from './types'

const BASE = '/api/v1'

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(
  path: string,
  apiKey: string,
  init?: RequestInit,
): Promise<T> {
  let response: Response
  try {
    response = await fetch(path, {
      ...init,
      headers: {
        'X-API-Key': apiKey,
        ...(init?.headers ?? {}),
      },
    })
  } catch (cause) {
    throw new ApiError(0, `Network error reaching the API (${String(cause)})`)
  }

  if (!response.ok) {
    // The API returns a uniform {"error": ..., "status": ...} envelope.
    let detail = `${response.status} ${response.statusText}`
    try {
      const body = (await response.json()) as { error?: unknown }
      if (body && typeof body.error === 'string') detail = body.error
    } catch {
      /* non-JSON error body — keep the status text */
    }
    throw new ApiError(response.status, detail)
  }

  return (await response.json()) as T
}

export const api = {
  // /healthz is deliberately outside /api/v1: it is unauthenticated.
  health: (key: string) => request<HealthOut>('/healthz', key),
  latestTelemetry: (key: string) => request<TelemetryOut>(`${BASE}/telemetry/latest`, key),
  stats: (key: string, hours = 24) =>
    request<WindowStats>(`${BASE}/telemetry/stats?hours=${hours}`, key),
  latestSchedule: (key: string) =>
    request<SchedulePlanOut>(`${BASE}/schedule/latest?action=run`, key),
  latestPlants: (key: string) => request<PlantOut[]>(`${BASE}/plant/latest`, key),
  latestAdvice: (key: string) => request<AdviceOut>(`${BASE}/orchestrator/latest`, key),
  runOptimizer: (key: string) =>
    request<OptimizeRunResponse>(`${BASE}/optimize/run`, key, { method: 'POST' }),
  runOrchestrator: (key: string) =>
    request<OrchestratorRunResponse>(`${BASE}/orchestrator/run`, key, { method: 'POST' }),
}
