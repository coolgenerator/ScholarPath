import React, { useCallback } from 'react';
import { motion, useReducedMotion } from 'motion/react';
import { ImageWithFallback } from './figma/ImageWithFallback';
import { useIsMobile } from './ui/use-mobile';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from './ui/sheet';
import { useApp } from '../../context/AppContext';

interface NavItem {
  id: string;
  icon: string;
  labelKey: string;
  filled?: boolean;
  group: 'explore' | 'decide' | 'utility';
}

const NAV_ITEMS: NavItem[] = [
  { id: 'advisor',     icon: 'chat_bubble',  labelKey: 'nav_advisor',     group: 'explore' },
  { id: 'school-list', icon: 'school',       labelKey: 'nav_school_list', group: 'explore', filled: true },
  { id: 'discover',    icon: 'explore',      labelKey: 'nav_discover',    group: 'explore' },
  { id: 'offers',      icon: 'local_offer',  labelKey: 'nav_offers',      group: 'decide' },
  { id: 'decisions',   icon: 'gavel',        labelKey: 'nav_decisions',   group: 'decide' },
  { id: 'history',     icon: 'history',      labelKey: 'nav_history',     group: 'utility' },
];

const GROUP_LABEL_KEYS: Record<string, string> = {
  explore: 'nav_explore',
  decide: 'nav_decide',
};

function SidebarBody({
  activeNav,
  collapsed,
  isMobile,
  onNavigate,
  onNewSession,
  onCollapse,
  studentName,
  t,
}: {
  activeNav: string;
  collapsed: boolean;
  isMobile: boolean;
  onNavigate: (nav: string) => void;
  onNewSession: () => void;
  onCollapse?: () => void;
  studentName: string | null;
  t: Record<string, string>;
}) {
  const reduceMotion = useReducedMotion();
  let lastGroup: string | null = null;

  return (
    <>
      {!isMobile && onCollapse && (
        <button
          onClick={onCollapse}
          className="absolute -right-3 top-20 z-50 flex h-6 w-6 items-center justify-center rounded-full border border-outline-variant/20 bg-white shadow-sm transition-colors hover:bg-surface-container-high"
        >
          <span className="material-symbols-outlined text-xs text-on-surface-variant">
            {collapsed ? 'chevron_right' : 'chevron_left'}
          </span>
        </button>
      )}

      <motion.div
        className={`${collapsed ? 'px-3' : 'px-6'} mb-8`}
        initial={reduceMotion ? { opacity: 0 } : { opacity: 0, y: 14 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: reduceMotion ? 0.18 : 0.42, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className="flex items-center gap-3">
          <div className="h-10 w-10 overflow-hidden rounded-xl bg-primary shadow-lg shadow-primary/20">
            <ImageWithFallback
              alt={t.nav_logo_alt}
              className="h-full w-full object-cover"
              src="https://lh3.googleusercontent.com/aida-public/AB6AXuC5f4biEkJI4CDgcAe3JY3qUjtnqQLQEw5yDYr_ZGxLOtVyPnnwCGRugDmAErA_5bUL5GR_DPD7LCMLn3qRCCvM0PfiCIxhfPX3kF24ccB9S_HMzI1hP_SshFq5zQN0zwe-tJTO4uhWNumBfD1NhSOa5WBPUepwhp4pZRf3Mp-zuwTtG089FDUGZlpBAA-SEfJ_KYNOVwedyAoph4DE1Z2u7UtcqDJo0KW6w5-p9fT05rNwM0-GuIasjDO8Gx3a8ZtznypTjr4gblY"
            />
          </div>
          {!collapsed && (
            <div>
              <div className="font-headline text-lg font-black leading-none text-[#191c1d]">ScholarPath</div>
              <div className="mt-1 text-[9px] font-bold uppercase tracking-widest text-on-surface-variant">
                {studentName ?? t.nav_tagline}
              </div>
            </div>
          )}
        </div>
      </motion.div>

      <nav className="min-h-0 flex-1 space-y-0.5 overflow-y-auto px-4">
        {NAV_ITEMS.map((item, index) => {
          const isActive = item.id === activeNav;
          const showGroupLabel = item.group !== 'utility' && item.group !== lastGroup;
          lastGroup = item.group;

          return (
            <React.Fragment key={item.id}>
              {showGroupLabel && !collapsed && (
                <div className="px-4 pb-2 pt-5">
                  <span className="text-[9px] font-bold uppercase tracking-[0.15em] text-on-surface-variant/50">
                    {(t as Record<string, unknown>)[GROUP_LABEL_KEYS[item.group]] as string}
                  </span>
                </div>
              )}
              {item.group === 'utility' && NAV_ITEMS[index - 1]?.group !== 'utility' && (
                <div className="my-3 border-t border-outline-variant/15" />
              )}
              <a
                className={
                  isActive
                    ? 'relative flex items-center gap-4 rounded-xl border border-outline-variant/10 bg-surface-container-lowest px-4 py-3 text-primary shadow-sm transition-all duration-200'
                    : 'group relative flex items-center gap-4 rounded-xl px-4 py-3 text-on-surface-variant transition-all duration-200 hover:bg-surface-container hover:text-on-surface'
                }
                href="#"
                onClick={(event) => {
                  event.preventDefault();
                  onNavigate(item.id);
                }}
              >
                {isActive && (
                  <motion.span
                    layoutId="sidebar-active-pill"
                    className="absolute inset-0 rounded-xl border border-primary/10 bg-white/90 shadow-[0_18px_32px_rgba(15,23,42,0.08)]"
                    transition={{ duration: reduceMotion ? 0.16 : 0.32, ease: [0.22, 1, 0.36, 1] }}
                  />
                )}
                <span
                  className="material-symbols-outlined relative z-[1] text-[22px]"
                  style={isActive && item.filled ? { fontVariationSettings: "'FILL' 1" } : undefined}
                >
                  {item.icon}
                </span>
                {!collapsed && (
                  <span className={`relative z-[1] font-headline text-sm ${isActive ? 'font-bold' : 'font-semibold'}`}>
                    {(t as Record<string, unknown>)[item.labelKey] as string}
                  </span>
                )}
              </a>
            </React.Fragment>
          );
        })}
      </nav>

      <div className="relative z-[60] shrink-0 px-4 pb-4">
        <button
          onClick={onNewSession}
          className="dashboard-hover-lift flex w-full items-center justify-center gap-2 rounded-xl bg-primary px-4 py-3 text-sm font-bold text-on-primary shadow-md transition-all hover:brightness-110"
        >
          <span className="material-symbols-outlined text-sm">add</span>
          {!collapsed && t.nav_new_session}
        </button>
      </div>
    </>
  );
}

interface SidebarProps {
  mobileOpen: boolean;
  onMobileOpenChange: (open: boolean) => void;
}

export function Sidebar({ mobileOpen, onMobileOpenChange }: SidebarProps) {
  const { activeNav, setActiveNav, studentName, clearSession, sidebarCollapsed, toggleSidebar, t } = useApp();
  const isMobile = useIsMobile();
  const collapsed = isMobile ? true : sidebarCollapsed;

  const handleNewSession = useCallback(() => {
    clearSession();
    setActiveNav('advisor');
    if (isMobile) {
      onMobileOpenChange(false);
    }
  }, [clearSession, setActiveNav, isMobile, onMobileOpenChange]);

  const handleNavigate = useCallback((nav: string) => {
    if (nav === 'advisor') {
      clearSession();
    }
    setActiveNav(nav);
    if (isMobile) {
      onMobileOpenChange(false);
    }
  }, [isMobile, onMobileOpenChange, setActiveNav, clearSession]);

  if (isMobile) {
    return (
      <Sheet open={mobileOpen} onOpenChange={onMobileOpenChange}>
        <SheetContent
          side="left"
          className="w-[288px] max-w-[86vw] border-r border-outline-variant/20 bg-surface-container-low p-0 sm:max-w-[288px]"
        >
          <SheetHeader className="sr-only">
            <SheetTitle>{t.nav_menu_title}</SheetTitle>
            <SheetDescription>{t.nav_menu_desc}</SheetDescription>
          </SheetHeader>
          <div className="relative flex h-full flex-col py-6">
            <SidebarBody
              activeNav={activeNav}
              collapsed={false}
              isMobile
              onNavigate={handleNavigate}
              onNewSession={handleNewSession}
              studentName={studentName}
              t={t}
            />
          </div>
        </SheetContent>
      </Sheet>
    );
  }

  return (
    <aside className={`fixed left-0 top-0 bottom-0 z-50 flex h-full flex-col border-r border-outline-variant/20 bg-surface-container-low py-6 transition-all duration-300 ${collapsed ? 'w-[72px]' : 'w-64'}`}>
      <SidebarBody
        activeNav={activeNav}
        collapsed={collapsed}
        isMobile={false}
        onNavigate={handleNavigate}
        onNewSession={handleNewSession}
        onCollapse={toggleSidebar}
        studentName={studentName}
        t={t}
      />
    </aside>
  );
}
