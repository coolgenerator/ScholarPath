import React, { useCallback } from 'react';
import { ImageWithFallback } from './figma/ImageWithFallback';
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

export function Sidebar() {
  const { activeNav, setActiveNav, studentName, setStudentId, setStudentName, setSessionId, sidebarCollapsed, toggleSidebar, t } = useApp();

  const handleNewSession = useCallback(() => {
    // Clear session — a new sessionId will be generated on first message
    setSessionId('');
    setActiveNav('advisor');
  }, [setSessionId, setActiveNav]);

  let lastGroup: string | null = null;

  return (
    <aside className={`flex flex-col ${sidebarCollapsed ? 'w-[72px]' : 'w-64'} h-full py-8 bg-surface-container-low border-r border-outline-variant/20 fixed left-0 top-0 bottom-0 z-50 transition-all duration-300`}>
      {/* Collapse toggle */}
      <button
        onClick={toggleSidebar}
        className="absolute -right-3 top-20 w-6 h-6 rounded-full bg-white border border-outline-variant/20 shadow-sm flex items-center justify-center z-50 hover:bg-surface-container-high transition-colors"
      >
        <span className="material-symbols-outlined text-xs text-on-surface-variant">
          {sidebarCollapsed ? 'chevron_right' : 'chevron_left'}
        </span>
      </button>
      {/* Logo */}
      <div className={`${sidebarCollapsed ? 'px-3' : 'px-6'} mb-10`}>
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-primary flex items-center justify-center rounded-xl overflow-hidden shadow-lg shadow-primary/20">
            <ImageWithFallback
              alt="ScholarPath Logo"
              className="w-full h-full object-cover"
              src="https://lh3.googleusercontent.com/aida-public/AB6AXuC5f4biEkJI4CDgcAe3JY3qUjtnqQLQEw5yDYr_ZGxLOtVyPnnwCGRugDmAErA_5bUL5GR_DPD7LCMLn3qRCCvM0PfiCIxhfPX3kF24ccB9S_HMzI1hP_SshFq5zQN0zwe-tJTO4uhWNumBfD1NhSOa5WBPUepwhp4pZRf3Mp-zuwTtG089FDUGZlpBAA-SEfJ_KYNOVwedyAoph4DE1Z2u7UtcqDJo0KW6w5-p9fT05rNwM0-GuIasjDO8Gx3a8ZtznypTjr4gblY"
            />
          </div>
          {!sidebarCollapsed && (
            <div>
              <div className="text-lg font-black text-[#191c1d] leading-none font-headline">ScholarPath</div>
              <div className="text-[9px] mt-1 uppercase tracking-widest text-on-surface-variant font-bold">
                {studentName ?? t.nav_tagline}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-4 space-y-0.5 overflow-y-auto min-h-0">
        {NAV_ITEMS.map((item) => {
          const isActive = activeNav === item.id;
          const showGroupLabel = item.group !== 'utility' && item.group !== lastGroup;
          lastGroup = item.group;

          return (
            <React.Fragment key={item.id}>
              {showGroupLabel && !sidebarCollapsed && (
                <div className="px-4 pt-5 pb-2">
                  <span className="text-[9px] font-bold text-on-surface-variant/50 uppercase tracking-[0.15em]">
                    {(t as Record<string, unknown>)[GROUP_LABEL_KEYS[item.group]] as string}
                  </span>
                </div>
              )}
              {item.group === 'utility' && NAV_ITEMS[NAV_ITEMS.indexOf(item) - 1]?.group !== 'utility' && (
                <div className="border-t border-outline-variant/15 my-3" />
              )}
              <a
                className={
                  isActive
                    ? 'flex items-center gap-4 px-4 py-3 bg-surface-container-lowest text-primary rounded-xl shadow-sm border border-outline-variant/10 transition-all duration-200'
                    : 'flex items-center gap-4 px-4 py-3 text-on-surface-variant hover:bg-surface-container hover:text-on-surface transition-all duration-200 rounded-xl group'
                }
                href="#"
                onClick={(e) => {
                  e.preventDefault();
                  setActiveNav(item.id);
                }}
              >
                <span
                  className="material-symbols-outlined text-[22px]"
                  style={isActive && item.filled ? { fontVariationSettings: "'FILL' 1" } : undefined}
                >
                  {item.icon}
                </span>
                {!sidebarCollapsed && (
                  <span className={`font-headline text-sm ${isActive ? 'font-bold' : 'font-semibold'}`}>
                    {(t as Record<string, unknown>)[item.labelKey] as string}
                  </span>
                )}
              </a>
            </React.Fragment>
          );
        })}
      </nav>

      {/* Bottom actions */}
      <div className="px-4 pb-4 shrink-0 relative z-[60]">
        <button
          onClick={handleNewSession}
          className="w-full py-3 px-4 bg-primary text-on-primary rounded-xl font-bold text-sm flex items-center justify-center gap-2 hover:brightness-110 transition-all shadow-md cursor-pointer"
        >
          <span className="material-symbols-outlined text-sm">add</span>
          {!sidebarCollapsed && t.nav_new_session}
        </button>
      </div>
    </aside>
  );
}
