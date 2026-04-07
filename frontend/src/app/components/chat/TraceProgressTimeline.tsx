import React from 'react';
import type { ChatProgressEvent } from '../../../hooks/useChat';

interface TraceProgressTimelineProps {
  events: ChatProgressEvent[];
  labelForEvent: (event: string) => string;
}

export function TraceProgressTimeline({ events, labelForEvent }: TraceProgressTimelineProps) {
  if (events.length === 0) return null;
  return (
    <div className="mt-1 w-full max-w-xl space-y-1 rounded-xl border border-outline-variant/10 bg-white/85 p-2 text-[11px] text-on-surface-variant/70">
      {events.slice(-4).map((event, idx) => (
        <div key={`${event.trace_id}-${event.timestamp}-${idx}`} className="flex items-center gap-2">
          <span className="h-1.5 w-1.5 rounded-full bg-primary/60"></span>
          <span>{labelForEvent(event.event)}</span>
        </div>
      ))}
    </div>
  );
}
