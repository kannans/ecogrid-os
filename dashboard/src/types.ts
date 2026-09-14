/**
 * Response shapes mirrored from ecogrid/schemas.py.
 *
 * These are deliberately hand-written rather than generated: they are few, they
 * change rarely, and keeping them explicit means a breaking API change shows up
 * as a type error here instead of an `undefined` in a component.
 */

export interface HealthOut {
  status: string
  version: string
  postgres: string
  redis: string
  consumer_lag_seconds: number | null
}

export interface TelemetryOut {
  window_from: string
  window_to: string
  forecast_intensity: number
  actual_intensity: number | null
  carbon_index: string
  generation_mix: Record<string, number>
  renewable_percentage: number
  low_carbon_percentage: number
  fossil_percentage: number
  is_forecast_only: boolean
  generation_mix_missing: boolean
  ingested_at: string
  revision_count: number
}

export interface WindowStats {
  window_count: number
  avg_renewable_percentage: number | null
  avg_fossil_percentage: number | null
  avg_actual_intensity: number | null
  cleanest_window_from: string | null
  dirtiest_window_from: string | null
  forecast_only_count: number
}

export interface ScheduleDecisionOut {
  run_id: string
  process_id: string
  process_name: string
  window_from: string
  window_to: string
  action: 'run' | 'idle'
  load_mw: number
  intensity: number
  carbon_kg: number
  baseline_carbon_kg: number
  carbon_saved_kg: number
  reason: string
}

export interface OptimizationRunOut {
  run_id: string
  created_at: string
  horizon_windows: number
  solver: string
  baseline_carbon_kg: number
  optimized_carbon_kg: number
  carbon_saved_kg: number
  process_count: number
  decision_count: number
  unscheduled: string[]
  notes: string[]
  saving_pct: number
}

export interface SchedulePlanOut {
  run: OptimizationRunOut
  count: number
  decisions: ScheduleDecisionOut[]
}

export interface PlantOut {
  plant_id: string
  plant_name: string
  window_from: string
  window_to: string
  total_load_mw: number
  flexible_load_mw: number
  inflexible_load_mw: number
  is_estimate: boolean
  revision_count: number
}

export interface AdviceOut {
  id: number
  run_id: string | null
  /** 'claude' | 'heuristic' — always surfaced so the user knows what advised. */
  source: string
  headline: string
  rationale: string
  confidence: number
  recommended_actions: string[]
  risk_flags: string[]
  created_at: string
}

export interface OptimizeRunResponse {
  run_id: string
  solver: string
  decision_count: number
  carbon_saved_kg: number
  saving_pct: number
  status: string
}

export interface OrchestratorRunResponse {
  run_id: string | null
  source: string
  headline: string
  rationale: string
  confidence: number
  recommended_actions: string[]
  risk_flags: string[]
  status: string
}
