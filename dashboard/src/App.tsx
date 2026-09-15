import { useCallback, useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { api, ApiError } from './api'
import type {
  AdviceOut,
  HealthOut,
  PlantOut,
  SchedulePlanOut,
  TelemetryOut,
  WindowStats,
} from './types'

interface Snapshot {
  health: HealthOut
  telemetry?: TelemetryOut
  stats?: WindowStats
  schedule?: SchedulePlanOut
  plants?: PlantOut[]
  advice?: AdviceOut
}

const KEY_STORAGE = 'ecogrid.apiKey'

/** Endpoints legitimately 404 before data exists — treat that as "not yet". */
async function optional<T>(promise: Promise<T>): Promise<T | undefined> {
  try {
    return await promise
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return undefined
    throw error
  }
}

const num = (value: number, digits = 0) =>
  value.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })

const shortTime = (iso: string) =>
  new Date(iso).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })

function Badge({ tone, label }: { tone: string; label: string }) {
  return <span className={`badge badge--${tone}`}>{label}</span>
}

function Card({
  title,
  subtitle,
  children,
}: {
  title: string
  subtitle?: string
  children: ReactNode
}) {
  return (
    <section className="card">
      <header className="card__head">
        <h2>{title}</h2>
        {subtitle ? <p className="card__sub">{subtitle}</p> : null}
      </header>
      <div className="card__body">{children}</div>
    </section>
  )
}

function Metric({ label, value, unit }: { label: string; value: string; unit?: string }) {
  return (
    <div className="metric">
      <span className="metric__label">{label}</span>
      <span className="metric__value">
        {value}
        {unit ? <em>{unit}</em> : null}
      </span>
    </div>
  )
}

export default function App() {
  const [apiKey, setApiKey] = useState(() => localStorage.getItem(KEY_STORAGE) ?? '')
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState<string | null>(null)
  const [toast, setToast] = useState<string | null>(null)

  const load = useCallback(async (key: string) => {
    setLoading(true)
    setError(null)
    try {
      const health = await api.health(key)
      const [telemetry, stats, schedule, plants, advice] = await Promise.all([
        optional(api.latestTelemetry(key)),
        optional(api.stats(key)),
        optional(api.latestSchedule(key)),
        optional(api.latestPlants(key)),
        optional(api.latestAdvice(key)),
      ])
      setSnapshot({ health, telemetry, stats, schedule, plants, advice })
    } catch (cause) {
      setSnapshot(null)
      setError(cause instanceof Error ? cause.message : String(cause))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (apiKey) void load(apiKey)
    // Only auto-load on mount; later loads are user-triggered.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const onLoad = () => {
    localStorage.setItem(KEY_STORAGE, apiKey)
    void load(apiKey)
  }

  const runAction = async (name: string, action: () => Promise<unknown>, message: string) => {
    setBusy(name)
    setToast(null)
    try {
      await action()
      setToast(message)
      await load(apiKey)
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause))
    } finally {
      setBusy(null)
    }
  }

  const { health, telemetry, stats, schedule, plants, advice } = snapshot ?? {}

  // Advice is generated on its own cadence (or on demand), so it can legitimately
  // refer to an older run than the schedule beside it. Showing "no optimisation
  // run available yet" next to a populated schedule reads as a contradiction, so
  // say plainly that the advice is behind rather than letting it look broken.
  const adviceStale = Boolean(advice && schedule && advice.run_id !== schedule.run.run_id)
  const scheduleRun = schedule?.run.run_id.slice(0, 8)

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <h1>EcoGrid OS</h1>
          <span>Industrial energy arbitrage &amp; decarbonization</span>
        </div>
        <div className="auth">
          <input
            type="password"
            placeholder="X-API-Key"
            value={apiKey}
            onChange={(event) => setApiKey(event.target.value)}
            onKeyDown={(event) => event.key === 'Enter' && onLoad()}
          />
          <button onClick={onLoad} disabled={loading || !apiKey}>
            {loading ? 'Loading…' : 'Load'}
          </button>
        </div>
      </header>

      {error ? <div className="banner banner--error">{error}</div> : null}
      {toast ? <div className="banner banner--ok">{toast}</div> : null}

      {!snapshot ? (
        <div className="empty">
          <p>Enter an API key to load the platform state.</p>
          <p className="hint">
            The bootstrap admin key is printed once by <code>migrate</code>:{' '}
            <code>docker compose logs migrate</code>
          </p>
        </div>
      ) : (
        <main className="grid">
          <Card title="Platform" subtitle={health ? `v${health.version}` : undefined}>
            <div className="row">
              <Badge
                tone={health && health.status === 'ok' ? 'good' : 'warn'}
                label={health ? health.status : 'unknown'}
              />
              <span className="muted">postgres {health?.postgres}</span>
              <span className="muted">redis {health?.redis}</span>
            </div>
            {health?.consumer_lag_seconds != null ? (
              <p className="muted">Newest window is {num(health.consumer_lag_seconds / 60, 1)} min old</p>
            ) : null}
          </Card>

          <Card title="Grid intensity" subtitle={telemetry ? shortTime(telemetry.window_from) : 'no data yet'}>
            {telemetry ? (
              <>
                <div className="row">
                  <Badge tone={telemetry.carbon_index} label={telemetry.carbon_index} />
                  <Metric
                    label="Intensity"
                    value={num(telemetry.actual_intensity ?? telemetry.forecast_intensity)}
                    unit="g/kWh"
                  />
                  <Metric label="Renewable" value={num(telemetry.renewable_percentage, 1)} unit="%" />
                </div>
                {stats ? (
                  <div className="row">
                    <Metric
                      label="Avg actual"
                      value={stats.avg_actual_intensity != null ? num(stats.avg_actual_intensity) : '—'}
                      unit="g/kWh"
                    />
                    <Metric label="Windows" value={num(stats.window_count)} />
                    <Metric label="Forecast-only" value={num(stats.forecast_only_count)} />
                  </div>
                ) : null}
              </>
            ) : (
              <p className="muted">No telemetry consumed yet — start the ingestion worker.</p>
            )}
          </Card>

          <Card
            title="Plant load"
            subtitle={plants?.length ? `${plants.length} plant(s) reporting` : 'no plant data yet'}
          >
            {plants?.length ? (
              <div className="row">
                <Metric label="Total" value={num(plants[0].total_load_mw, 1)} unit="MW" />
                <Metric label="Flexible" value={num(plants[0].flexible_load_mw, 1)} unit="MW" />
                <Metric label="Inflexible" value={num(plants[0].inflexible_load_mw, 1)} unit="MW" />
              </div>
            ) : (
              <p className="muted">Start <code>plant-bridge</code> and <code>plant-consumer</code>.</p>
            )}
          </Card>

          <Card
            title="Dispatch schedule"
            subtitle={schedule ? `${schedule.run.solver} · run ${schedule.run.run_id.slice(0, 8)}` : 'no run yet'}
          >
            {schedule ? (
              <>
                <div className="row">
                  <Metric label="Saved" value={num(schedule.run.carbon_saved_kg)} unit="kg CO₂e" />
                  <Metric label="Reduction" value={num(schedule.run.saving_pct, 1)} unit="%" />
                  <Metric label="Decisions" value={num(schedule.count)} />
                </div>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Process</th>
                      <th>Window</th>
                      <th>g/kWh</th>
                      <th>Saved</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schedule.decisions.slice(0, 8).map((decision) => (
                      <tr key={`${decision.process_id}-${decision.window_from}`}>
                        <td>{decision.process_name || decision.process_id}</td>
                        <td>{shortTime(decision.window_from)}</td>
                        <td>{num(decision.intensity)}</td>
                        <td>{num(decision.carbon_saved_kg)} kg</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            ) : (
              <p className="muted">Trigger an optimization run to see a schedule.</p>
            )}
            <div className="actions">
              <button
                disabled={busy !== null}
                onClick={() =>
                  runAction('optimize', () => api.runOptimizer(apiKey), 'Optimization run complete')
                }
              >
                {busy === 'optimize' ? 'Running…' : 'Run optimizer'}
              </button>
            </div>
          </Card>

          <Card
            title="AI Orchestrator"
            subtitle={
              advice
                ? `source: ${advice.source}${adviceStale ? ' · predates current schedule' : ''}`
                : 'no advice yet'
            }
          >
            {advice ? (
              <>
                <div className="row">
                  <Badge tone={advice.source === 'claude' ? 'good' : 'warn'} label={advice.source} />
                  <Metric label="Confidence" value={num(advice.confidence * 100, 0)} unit="%" />
                </div>
                {adviceStale ? (
                  <p className="stale">
                    This advice was written for run {advice.run_id?.slice(0, 8) ?? 'none'}, but the
                    current schedule is run {scheduleRun}. Press <strong>Ask orchestrator</strong> to
                    re-review it.
                  </p>
                ) : null}
                <p className="headline">{advice.headline}</p>
                <p className="muted">{advice.rationale}</p>
                {advice.recommended_actions.length ? (
                  <ul className="list">
                    {advice.recommended_actions.map((action) => (
                      <li key={action}>{action}</li>
                    ))}
                  </ul>
                ) : null}
                {advice.risk_flags.length ? (
                  <ul className="list list--risk">
                    {advice.risk_flags.map((flag) => (
                      <li key={flag}>{flag}</li>
                    ))}
                  </ul>
                ) : null}
              </>
            ) : (
              <p className="muted">No advice yet — ask the orchestrator.</p>
            )}
            <div className="actions">
              <button
                disabled={busy !== null}
                onClick={() =>
                  runAction('advise', () => api.runOrchestrator(apiKey), 'Orchestrator pass complete')
                }
              >
                {busy === 'advise' ? 'Thinking…' : 'Ask orchestrator'}
              </button>
            </div>
          </Card>
        </main>
      )}
    </div>
  )
}
