import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { TurnTraceStep } from '../../../lib/types';
import type { TracePanelMode, TurnTraceView } from '../../../hooks/useChat';
import { useApp } from '../../../context/AppContext';
import { locales } from '../../../i18n';

type Translator = typeof locales.en;

function formatTime(timestamp: string): string {
  try {
    return new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  } catch {
    return '';
  }
}

function formatDuration(startedAt?: string | null, endedAt?: string | null): string {
  if (!startedAt || !endedAt) return '—';
  const start = Date.parse(startedAt);
  const end = Date.parse(endedAt);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return '—';
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function statusLabel(status: TurnTraceView['status'], t: Translator): string {
  if (status === 'ok') return t.chat_trace_status_ok;
  if (status === 'error') return t.chat_trace_status_error;
  return t.chat_trace_status_running;
}

function statusTone(status?: string | null): string {
  if (status === 'completed') return 'text-emerald-700';
  if (status === 'running' || status === 'queued' || status === 'retrying') return 'text-primary';
  if (status === 'timeout' || status === 'blocked' || status === 'cancelled') return 'text-amber-700';
  if (status === 'failed') return 'text-error';
  return 'text-on-surface-variant/70';
}

function eventLabel(event: TurnTraceStep['event'], t: Translator): string {
  if (event === 'turn_started') return t.chat_trace_event_turn_started;
  if (event === 'planning_done') return t.chat_trace_event_planning_done;
  if (event === 'capability_started') return t.chat_trace_event_capability_started;
  if (event === 'capability_finished') return t.chat_trace_event_capability_finished;
  if (event === 'rollback') return t.chat_trace_event_rollback;
  if (event === 'turn_completed') return t.chat_trace_event_turn_completed;
  return event;
}

function stepStatusLabel(status: NonNullable<TurnTraceStep['step_status']>, t: Translator): string {
  if (status === 'queued') return t.chat_trace_step_status_queued;
  if (status === 'running') return t.chat_trace_step_status_running;
  if (status === 'completed') return t.chat_trace_step_status_completed;
  if (status === 'failed') return t.chat_trace_step_status_failed;
  if (status === 'blocked') return t.chat_trace_step_status_blocked;
  if (status === 'cancelled') return t.chat_trace_step_status_cancelled;
  if (status === 'timeout') return t.chat_trace_step_status_timeout;
  if (status === 'retrying') return t.chat_trace_step_status_retrying;
  return status;
}

function stepTitle(step: TurnTraceStep, t: Translator): string {
  if (step.display?.title && step.display.title.trim()) return step.display.title;
  if (step.capability_id) return step.capability_id;
  return eventLabel(step.event, t);
}

function stepStateText(step: TurnTraceStep, t: Translator): string {
  if (step.step_status) return stepStatusLabel(step.step_status, t);
  return eventLabel(step.event, t);
}

interface TraceWaveGroup {
  wave: number;
  checkpoints: TurnTraceStep[];
  capabilities: TurnTraceStep[];
  others: TurnTraceStep[];
}

function groupWaves(steps: TurnTraceStep[]): TraceWaveGroup[] {
  const grouped = new Map<number, TraceWaveGroup>();
  const orderedSteps = [...steps].sort((a, b) => {
    const seqA = typeof a.event_seq === 'number' ? a.event_seq : 0;
    const seqB = typeof b.event_seq === 'number' ? b.event_seq : 0;
    if (seqA !== seqB) return seqA - seqB;
    return Date.parse(a.timestamp) - Date.parse(b.timestamp);
  });

  orderedSteps.forEach((step) => {
    const wave = typeof step.wave_index === 'number' ? step.wave_index : -1;
    const current = grouped.get(wave) ?? {
      wave,
      checkpoints: [],
      capabilities: [],
      others: [],
    };
    if (step.step_kind === 'checkpoint') {
      current.checkpoints.push(step);
    } else if (step.step_kind === 'capability') {
      current.capabilities.push(step);
    } else {
      current.others.push(step);
    }
    grouped.set(wave, current);
  });

  return [...grouped.values()].sort((a, b) => a.wave - b.wave);
}

interface ExecutionTracePanelProps {
  activeTrace: TurnTraceView | null;
  activeTraceView?: 'compact' | 'full';
  isTraceLoading: boolean;
  tracePanelMode?: TracePanelMode;
  onTracePanelModeChange?: (mode: TracePanelMode, traceId?: string | null) => void;
  onLoadFullTrace?: (traceId: string) => void;
}

const COMPACT_STEP_LIMIT = 60;

export function ExecutionTracePanel({
  activeTrace,
  activeTraceView = 'compact',
  isTraceLoading,
  tracePanelMode = 'auto_collapse_on_finish',
  onTracePanelModeChange,
  onLoadFullTrace,
}: ExecutionTracePanelProps) {
  const { t } = useApp();
  const [expanded, setExpanded] = useState(false);
  const [showTechnicalDetails, setShowTechnicalDetails] = useState(false);
  const latestTraceIdRef = useRef<string | null>(null);

  useEffect(() => {
    if (!activeTrace) {
      latestTraceIdRef.current = null;
      setExpanded(false);
      return;
    }

    const traceChanged = latestTraceIdRef.current !== activeTrace.trace_id;
    latestTraceIdRef.current = activeTrace.trace_id;

    if (!traceChanged && tracePanelMode === 'user_pinned') {
      return;
    }

    if (traceChanged) {
      setShowTechnicalDetails(false);
      if (activeTrace.status === 'running') {
        setExpanded(true);
        return;
      }
      setExpanded(false);
      return;
    }

    if (tracePanelMode === 'auto_expand_on_running' && activeTrace.status === 'running') {
      setExpanded(true);
      return;
    }

    if (tracePanelMode === 'auto_collapse_on_finish' && activeTrace.status !== 'running') {
      setExpanded(false);
    }
  }, [activeTrace, tracePanelMode]);

  const traceSummary = useMemo(() => {
    if (!activeTrace) return null;
    return {
      waveCount: Number(activeTrace.usage?.wave_count ?? 0) || 0,
      toolSteps: Number(activeTrace.usage?.tool_steps_used ?? activeTrace.usage?.tool_calls ?? 0) || 0,
      lockReject: Boolean(activeTrace.usage?.rejected_by_lock),
      degradedCount: Number(activeTrace.usage?.best_effort_degraded_count ?? 0) || 0,
      duration: formatDuration(activeTrace.started_at, activeTrace.ended_at),
    };
  }, [activeTrace]);

  const compactSteps = useMemo(() => {
    if (!activeTrace) return [] as TurnTraceStep[];
    if (activeTraceView === 'full') return activeTrace.steps;
    return activeTrace.steps.slice(-COMPACT_STEP_LIMIT);
  }, [activeTrace, activeTraceView]);

  const hasMoreSteps = Boolean(
    activeTrace
    && activeTraceView !== 'full'
    && activeTrace.step_count > compactSteps.length,
  );

  const waves = useMemo(() => {
    if (!activeTrace) return [] as TraceWaveGroup[];
    return groupWaves(compactSteps);
  }, [activeTrace, compactSteps]);

  if (!activeTrace) return null;

  const handlePanelToggle = () => {
    setExpanded((prev) => !prev);
    onTracePanelModeChange?.('user_pinned', activeTrace.trace_id);
  };

  const isRunning = activeTrace.status === 'running';
  const statusIcon = isRunning ? 'pending' : activeTrace.status === 'ok' ? 'check_circle' : 'error';
  const statusColor = isRunning ? 'text-primary' : activeTrace.status === 'ok' ? 'text-emerald-600' : 'text-error';

  return (
    <div className="flex w-full justify-start">
      <div className={`rounded-xl border border-outline-variant/10 bg-surface-container-lowest/80 backdrop-blur transition-all ${expanded ? 'w-full max-w-2xl p-3' : 'px-3 py-1.5'}`}>
        <button
          onClick={handlePanelToggle}
          className="flex w-full items-center justify-between gap-2 text-left"
        >
          <div className="flex items-center gap-1.5 min-w-0">
            <span className={`material-symbols-outlined text-[14px] ${statusColor}`}>{statusIcon}</span>
            <span className="text-[10px] font-semibold uppercase tracking-[0.1em] text-on-surface-variant/60">
              {t.chat_trace_title}
            </span>
            {!expanded && (
              <span className="text-[10px] text-on-surface-variant/50">
                {traceSummary?.toolSteps ? `${traceSummary.toolSteps} tools` : ''} {traceSummary?.duration && traceSummary.duration !== '—' ? `· ${traceSummary.duration}` : ''}
              </span>
            )}
          </div>
          <span className="material-symbols-outlined text-[14px] text-on-surface-variant/50">
            {expanded ? 'expand_less' : 'expand_more'}
          </span>
        </button>

        {expanded && (
          <div className="mt-3 space-y-3">
            <div className="flex items-center justify-end">
              <button
                onClick={() => setShowTechnicalDetails((prev) => !prev)}
                className="rounded-full border border-outline-variant/20 px-2.5 py-1 text-[11px] font-semibold text-on-surface-variant/80 transition hover:border-primary/30 hover:text-primary"
              >
                {showTechnicalDetails ? t.chat_trace_hide_technical : t.chat_trace_show_technical}
              </button>
            </div>
            {hasMoreSteps && (
              <div className="flex items-center justify-between rounded-xl border border-outline-variant/10 bg-white/85 px-2.5 py-2 text-[11px] text-on-surface-variant/70">
                <span>{t.chat_trace_compact_hint(compactSteps.length, activeTrace.step_count)}</span>
                <button
                  onClick={() => onLoadFullTrace?.(activeTrace.trace_id)}
                  className="rounded-full border border-primary/20 px-2.5 py-1 text-[11px] font-semibold text-primary transition hover:bg-primary/10"
                >
                  {t.chat_trace_load_full}
                </button>
              </div>
            )}
            {waves.map(({ wave, checkpoints, capabilities, others }) => (
              <div key={`wave-${wave}`} className="rounded-xl border border-outline-variant/10 bg-surface/20 p-2.5">
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-on-surface-variant/65">
                  {wave >= 0 ? t.chat_trace_wave_label(wave) : t.chat_trace_global_label}
                </div>
                <div className="space-y-2">
                  {checkpoints.length > 0 && (
                    <div className="rounded-lg border border-outline-variant/10 bg-white/85 p-2">
                      <div className="mb-1 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/55">
                        {t.chat_trace_section_checkpoint}
                      </div>
                      <div className="space-y-1.5">
                        {checkpoints.map((step) => {
                          const summary = step.checkpoint_summary && typeof step.checkpoint_summary === 'object'
                            ? (step.checkpoint_summary as Record<string, unknown>)
                            : null;
                          return (
                            <div key={`${step.step_id}-${step.event_seq ?? step.event}`} className="rounded-md border border-outline-variant/10 px-2 py-1.5 text-xs">
                              <div className="flex items-center justify-between gap-2">
                                <div className="font-medium text-on-surface">{stepTitle(step, t)}</div>
                                <span className={`text-[11px] ${statusTone(step.step_status)}`}>
                                  {stepStateText(step, t)}
                                </span>
                              </div>
                              {summary && (
                                <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-on-surface-variant/70">
                                  <span>{t.chat_trace_summary_added(Number(summary.added_count ?? 0))}</span>
                                  <span>{t.chat_trace_summary_dropped(Number(summary.dropped_count ?? 0))}</span>
                                  <span>{t.chat_trace_summary_reprioritized(Number(summary.reprioritized_count ?? 0))}</span>
                                  <span>{String(summary.profile_gate_status ?? t.chat_trace_summary_profile_gate_na)}</span>
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                  )}

                  {capabilities.length > 0 && (
                    <div className="rounded-lg border border-outline-variant/10 bg-white/85 p-2">
                      <div className="mb-1 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/55">
                        {t.chat_trace_section_capability}
                      </div>
                      <div className="space-y-1.5">
                        {capabilities.map((step) => (
                          <div key={`${step.step_id}-${step.event_seq ?? step.event}`} className="flex items-center justify-between gap-3 rounded-md border border-outline-variant/10 px-2 py-1.5 text-xs">
                            <div className="min-w-0">
                              <div className="font-medium text-on-surface">{stepTitle(step, t)}</div>
                              <div className={`text-[11px] ${statusTone(step.step_status)}`}>
                                {stepStateText(step, t)}
                                {(showTechnicalDetails && step.display?.badge) ? ` · ${step.display.badge}` : ''}
                                {(showTechnicalDetails && step.compact_reason_code) ? ` · ${step.compact_reason_code}` : ''}
                              </div>
                              {step.data && typeof step.data === 'object' && typeof (step.data as Record<string, unknown>).what_done === 'string' && (
                                <div className="mt-0.5 text-[11px] text-on-surface-variant/70">
                                  {(step.data as Record<string, unknown>).what_done as string}
                                </div>
                              )}
                              {showTechnicalDetails && step.data
                                && typeof step.data === 'object'
                                && Array.isArray((step.data as Record<string, unknown>).needs_input)
                                && ((step.data as Record<string, unknown>).needs_input as unknown[])
                                  .map((item) => String(item).trim())
                                  .filter((item) => item.length > 0)
                                  .length > 0 && (
                                <div className="mt-0.5 text-[11px] text-on-surface-variant/62">
                                  {((step.data as Record<string, unknown>).needs_input as unknown[])
                                    .map((item) => String(item).trim())
                                    .filter((item) => item.length > 0)
                                    .slice(0, 2)
                                    .join(' · ')}
                                </div>
                              )}
                            </div>
                            <div className="shrink-0 text-[11px] text-on-surface-variant/65">
                              {typeof step.duration_ms === 'number' ? `${step.duration_ms}ms` : formatTime(step.timestamp)}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {others.length > 0 && (
                    <div className="rounded-lg border border-outline-variant/10 bg-white/85 p-2">
                      <div className="mb-1 text-[10px] font-bold uppercase tracking-[0.12em] text-on-surface-variant/55">
                        {t.chat_trace_section_turn}
                      </div>
                      <div className="space-y-1.5">
                        {others.map((step) => (
                          <div key={`${step.step_id}-${step.event_seq ?? step.event}`} className="rounded-md border border-outline-variant/10 px-2 py-1.5 text-xs">
                            <div className="flex items-center justify-between gap-2">
                              <div className="font-medium text-on-surface">{stepTitle(step, t)}</div>
                              <span className={`text-[11px] ${statusTone(step.step_status)}`}>
                                {stepStateText(step, t)}
                              </span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
