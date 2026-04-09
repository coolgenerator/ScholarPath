import { AppProvider, useApp } from '../../context/AppContext';
import { ProfilePanel } from '../components/ProfilePanel';
import { Link } from 'react-router';

function ProfilePageInner() {
  const { studentId, t } = useApp();

  return (
    <div className="min-h-screen bg-background text-on-surface font-body">
      <header className="flex h-14 items-center justify-between border-b border-outline-variant/10 bg-background/80 px-6 backdrop-blur-md">
        <Link
          to="/s/new/advisor"
          className="flex items-center gap-2 text-sm font-bold text-on-surface-variant/70 transition hover:text-on-surface"
        >
          <span className="material-symbols-outlined text-[18px]">arrow_back</span>
          {t.nav_back ?? '返回工作台'}
        </Link>
      </header>
      <div className="mx-auto max-w-4xl">
        <ProfilePanel studentId={studentId} />
      </div>
    </div>
  );
}

export function ProfilePage() {
  return (
    <AppProvider>
      <ProfilePageInner />
    </AppProvider>
  );
}
