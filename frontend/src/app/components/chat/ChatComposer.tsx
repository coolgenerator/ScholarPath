import React from 'react';

interface ChatComposerProps {
  value: string;
  placeholder: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onKeyDown: (event: React.KeyboardEvent) => void;
}

export function ChatComposer({
  value,
  placeholder,
  onChange,
  onSend,
  onKeyDown,
}: ChatComposerProps) {
  return (
    <div className="sticky bottom-0 z-20 bg-gradient-to-t from-white via-white/96 to-white/0 px-4 pb-4 pt-4 sm:px-6 sm:pb-6 lg:px-8">
      <div className="mx-auto w-full" style={{ maxWidth: '960px' }}>
        <div className="relative flex items-center rounded-[1.75rem] border border-outline-variant/15 bg-white/92 px-4 py-3 shadow-[0_22px_52px_rgba(15,23,42,0.12)] backdrop-blur transition-all duration-300 focus-within:-translate-y-0.5 focus-within:border-primary/20 focus-within:shadow-[0_26px_64px_rgba(0,64,161,0.14)] sm:px-5 sm:py-4">
          <div className="pointer-events-none absolute inset-x-8 top-0 h-px bg-gradient-to-r from-transparent via-primary/25 to-transparent"></div>
          <input
            className="flex-1 border-none bg-transparent py-1 text-sm text-on-surface placeholder:text-on-surface-variant/55 outline-none focus:ring-0"
            placeholder={placeholder}
            type="text"
            value={value}
            onChange={(event) => onChange(event.target.value)}
            onKeyDown={onKeyDown}
          />
          <button
            onClick={onSend}
            disabled={!value.trim()}
            className="ml-3 flex h-11 w-11 items-center justify-center rounded-2xl bg-primary text-on-primary shadow-[0_16px_34px_rgba(3,2,19,0.22)] transition-all duration-300 hover:-translate-y-0.5 hover:scale-[1.03] disabled:opacity-50 disabled:hover:translate-y-0 disabled:hover:scale-100"
          >
            <span className="material-symbols-outlined text-sm font-bold">arrow_upward</span>
          </button>
        </div>
      </div>
    </div>
  );
}
