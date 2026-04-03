import React from 'react';
import { useApp } from '../../context/AppContext';

type PageFallbackVariant = 'advisor' | 'dashboard';

const PAGE_TITLE_MAP: Record<string, (t: ReturnType<typeof useApp>['t']) => string> = {
  advisor: (t) => t.nav_advisor,
  'school-list': (t) => t.sl_title,
  discover: (t) => t.disc_title,
  offers: (t) => t.off_title,
  decisions: (t) => t.dec_title,
  history: (t) => t.hist_title,
  profile: (t) => t.prof_title,
};

function getFallbackTitle(activeNav: string, t: ReturnType<typeof useApp>['t']): string {
  return PAGE_TITLE_MAP[activeNav]?.(t) ?? t.sl_title;
}

export function PageFallback({
  variant,
  activeNav,
}: {
  variant: PageFallbackVariant;
  activeNav: string;
}) {
  const { t } = useApp();
  const title = getFallbackTitle(activeNav, t);

  if (variant === 'advisor') {
    return (
      <section className="flex h-full w-full flex-col overflow-hidden bg-background font-body" aria-busy="true">
        <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
          <div className="mx-auto flex h-full w-full max-w-[960px] flex-col justify-between gap-8">
            <div className="space-y-4">
              <div className="h-4 w-36 animate-pulse rounded-full bg-surface-container-high/60" />
              <div className="h-28 animate-pulse rounded-[2rem] bg-surface-container-high/40" />
              <div className="ml-auto h-16 w-[52%] animate-pulse rounded-[1.75rem] bg-surface-container-high/50" />
              <div className="h-20 w-[74%] animate-pulse rounded-[1.75rem] bg-surface-container-high/50" />
            </div>

            <div className="rounded-[1.75rem] border border-outline-variant/10 bg-white px-5 py-4 shadow-sm">
              <div className="mb-4 flex items-center justify-between">
                <div className="h-3 w-28 animate-pulse rounded-full bg-surface-container-high/60" />
                <div className="h-9 w-9 animate-pulse rounded-full bg-surface-container-high/60" />
              </div>
              <div className="h-11 animate-pulse rounded-2xl bg-surface-container-high/50" />
            </div>
          </div>
        </div>
      </section>
    );
  }

  return (
    <section className="flex h-full w-full flex-col overflow-hidden bg-background font-body" aria-busy="true">
      <header className="sticky top-0 z-20 flex min-h-16 items-center border-b border-outline-variant/10 bg-background/90 px-4 py-3 backdrop-blur-md sm:px-6 lg:px-8">
        <div>
          <h1 className="font-headline text-lg font-black tracking-tight text-on-surface">{title}</h1>
          <p className="text-[9px] font-bold uppercase tracking-[0.1em] text-on-surface-variant">
            {t.common_loading_workspace}
          </p>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto px-4 py-5 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
        <div className="space-y-5">
          <div className="h-24 animate-pulse rounded-[1.75rem] bg-surface-container-high/45" />
          <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
            <div className="h-32 animate-pulse rounded-[1.6rem] bg-surface-container-high/50" />
            <div className="h-32 animate-pulse rounded-[1.6rem] bg-surface-container-high/50" />
            <div className="h-32 animate-pulse rounded-[1.6rem] bg-surface-container-high/50" />
          </div>
          <div className="space-y-3">
            {[...Array(4)].map((_, index) => (
              <div key={index} className="h-24 animate-pulse rounded-[1.6rem] bg-surface-container-high/40" />
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
