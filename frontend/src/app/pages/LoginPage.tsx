import React, { useEffect, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router';

import { MarketingFloat, MarketingReveal } from '../components/MarketingMotion';
import { DashboardInput } from '../components/ui/dashboard-input';
import { DashboardFieldLabel } from '../components/ui/dashboard-select';
import { authApi } from '../../lib/api/auth';
import { buildWorkspacePath, readWorkspaceSnapshot } from '../../lib/workspaceSession';

type Step = 'email' | 'otp';

const OTP_COOLDOWN = 60; // seconds

export function LoginPage() {
  const navigate = useNavigate();
  const [step, setStep] = useState<Step>('email');
  const [email, setEmail] = useState('');
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [countdown, setCountdown] = useState(0);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    document.title = '登录 | ScholarPath';
    document.documentElement.lang = 'zh-CN';
  }, []);

  // Countdown timer
  useEffect(() => {
    if (countdown <= 0) {
      if (timerRef.current) clearInterval(timerRef.current);
      return;
    }
    timerRef.current = setInterval(() => {
      setCountdown((c) => {
        if (c <= 1) {
          if (timerRef.current) clearInterval(timerRef.current);
          return 0;
        }
        return c - 1;
      });
    }, 1000);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [countdown]);

  async function handleRequestOtp(e?: React.FormEvent) {
    e?.preventDefault();
    if (!email.trim() || isLoading) return;

    setError(null);
    setIsLoading(true);
    try {
      await authApi.requestOtp(email.trim());
      setStep('otp');
      setCountdown(OTP_COOLDOWN);
    } catch (err) {
      setError(err instanceof Error ? err.message : '发送验证码失败，请重试');
    } finally {
      setIsLoading(false);
    }
  }

  async function handleVerify(e: React.FormEvent) {
    e.preventDefault();
    if (!code.trim() || isLoading) return;

    setError(null);
    setIsLoading(true);
    try {
      const res = await authApi.verifyOtp(email.trim(), code.trim());

      // Store auth tokens
      localStorage.setItem('sp_auth_token', res.access_token);
      localStorage.setItem('sp_user_id', res.user_id);
      if (res.student_id) {
        localStorage.setItem('sp_student_id', res.student_id);
      }

      // Navigate based on whether user has a student profile
      if (res.student_id) {
        const snapshot = readWorkspaceSnapshot();
        navigate(buildWorkspacePath(snapshot.sessionId, 'advisor'));
      } else {
        navigate('/register');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '验证失败，请检查验证码');
    } finally {
      setIsLoading(false);
    }
  }

  function handleSkip() {
    navigate(buildWorkspacePath(null, 'advisor'));
  }

  const emailValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());

  return (
    <div className="min-h-screen overflow-x-hidden bg-[radial-gradient(circle_at_top,rgba(23,48,75,0.16),transparent_26%),radial-gradient(circle_at_86%_12%,rgba(208,155,82,0.18),transparent_22%),linear-gradient(180deg,#f4eee4_0%,#ebe1cf_48%,#f8f3eb_100%)] text-[#10253d]">
      <div className="relative isolate">
        <MarketingFloat className="pointer-events-none absolute -left-20 top-24 hidden lg:block" y={16} x={12} duration={15}>
          <div className="h-64 w-64 rounded-full bg-[#17304b]/8 blur-3xl" />
        </MarketingFloat>
        <MarketingFloat className="pointer-events-none absolute right-0 top-8" y={12} x={-10} duration={11.5}>
          <div className="h-56 w-56 rounded-full bg-[#d09b52]/14 blur-3xl" />
        </MarketingFloat>

        <div className="mx-auto flex min-h-screen w-full max-w-7xl flex-col px-6 py-6 lg:px-10 lg:py-8">
          <header className="flex items-center justify-between">
            <Link to="/" className="flex items-center gap-3">
              <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-[#17304b] text-white shadow-[0_18px_40px_rgba(12,27,45,0.24)]">
                <span className="material-symbols-outlined text-[24px]">school</span>
              </div>
              <div>
                <div className="font-headline text-xl font-black tracking-tight">ScholarPath</div>
                <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-[#17304b]/52">登录你的账号</div>
              </div>
            </Link>

            <Link
              to="/"
              className="inline-flex items-center gap-2 rounded-full bg-white/76 px-4 py-2 text-sm font-bold text-[#17304b] shadow-[0_12px_28px_rgba(15,23,42,0.06)] backdrop-blur"
            >
              <span className="material-symbols-outlined text-[18px]">west</span>
              返回介绍页
            </Link>
          </header>

          <main className="flex flex-1 items-center justify-center py-8">
            <MarketingReveal
              mode="immediate"
              amount={26}
              scale={0.988}
              className="w-full max-w-md rounded-[2.15rem] border border-white/78 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(244,238,228,0.9))] p-6 shadow-[0_30px_72px_rgba(15,23,42,0.1)] sm:p-8"
            >
              <div className="mb-6">
                <div className="text-[11px] font-bold uppercase tracking-[0.18em] text-[#17304b]/54">
                  {step === 'email' ? '邮箱登录' : '输入验证码'}
                </div>
                <h2 className="mt-3 font-headline text-2xl font-black tracking-tight text-[#10253d]">
                  {step === 'email' ? '登录 ScholarPath' : '验证你的邮箱'}
                </h2>
                <p className="mt-2 text-sm leading-7 text-[#17304b]/66">
                  {step === 'email'
                    ? '输入你的邮箱地址，我们会发送一个验证码。'
                    : `验证码已发送至 ${email}，请查看邮箱。`}
                </p>
              </div>

              {step === 'email' ? (
                <form className="space-y-5" onSubmit={handleRequestOtp}>
                  <div className="space-y-2">
                    <DashboardFieldLabel htmlFor="login-email" className="text-[10px] tracking-[0.14em] text-[#17304b]/62">
                      邮箱地址
                    </DashboardFieldLabel>
                    <DashboardInput
                      id="login-email"
                      type="email"
                      value={email}
                      onChange={(e) => setEmail(e.target.value)}
                      placeholder="you@example.com"
                      autoFocus
                    />
                  </div>

                  {error && (
                    <div className="rounded-2xl border border-error/15 bg-error/6 px-4 py-3 text-sm text-error">
                      {error}
                    </div>
                  )}

                  <button
                    type="submit"
                    disabled={!emailValid || isLoading}
                    className="w-full rounded-full bg-[linear-gradient(135deg,#17304b,#0f2237)] px-6 py-4 text-sm font-black text-white shadow-[0_20px_44px_rgba(12,27,45,0.24)] transition hover:-translate-y-0.5 hover:brightness-110 disabled:translate-y-0 disabled:cursor-not-allowed disabled:opacity-55"
                  >
                    {isLoading ? '发送中...' : '发送验证码'}
                  </button>
                </form>
              ) : (
                <form className="space-y-5" onSubmit={handleVerify}>
                  <div className="space-y-2">
                    <DashboardFieldLabel htmlFor="login-code" className="text-[10px] tracking-[0.14em] text-[#17304b]/62">
                      6 位验证码
                    </DashboardFieldLabel>
                    <DashboardInput
                      id="login-code"
                      type="text"
                      inputMode="numeric"
                      maxLength={6}
                      value={code}
                      onChange={(e) => setCode(e.target.value.replace(/\D/g, '').slice(0, 6))}
                      placeholder="000000"
                      autoFocus
                    />
                  </div>

                  {error && (
                    <div className="rounded-2xl border border-error/15 bg-error/6 px-4 py-3 text-sm text-error">
                      {error}
                    </div>
                  )}

                  <button
                    type="submit"
                    disabled={code.length !== 6 || isLoading}
                    className="w-full rounded-full bg-[linear-gradient(135deg,#17304b,#0f2237)] px-6 py-4 text-sm font-black text-white shadow-[0_20px_44px_rgba(12,27,45,0.24)] transition hover:-translate-y-0.5 hover:brightness-110 disabled:translate-y-0 disabled:cursor-not-allowed disabled:opacity-55"
                  >
                    {isLoading ? '验证中...' : '验证'}
                  </button>

                  <div className="flex items-center justify-between text-sm">
                    <button
                      type="button"
                      onClick={() => {
                        setStep('email');
                        setCode('');
                        setError(null);
                      }}
                      className="font-semibold text-[#17304b]/55 transition hover:text-[#17304b]/80"
                    >
                      更换邮箱
                    </button>
                    <button
                      type="button"
                      disabled={countdown > 0 || isLoading}
                      onClick={() => handleRequestOtp()}
                      className="font-semibold text-[#17304b]/55 transition hover:text-[#17304b]/80 disabled:cursor-not-allowed disabled:opacity-45"
                    >
                      {countdown > 0 ? `${countdown}s 后可重新发送` : '重新发送验证码'}
                    </button>
                  </div>
                </form>
              )}

              <div className="mt-6 flex flex-col items-center gap-3 border-t border-[#17304b]/8 pt-5">
                <button
                  type="button"
                  onClick={handleSkip}
                  className="text-sm font-semibold text-[#17304b]/55 transition hover:text-[#17304b]/80"
                >
                  跳过，直接体验 →
                </button>
                <Link
                  to="/register"
                  className="text-sm font-semibold text-[#17304b]/55 transition hover:text-[#17304b]/80"
                >
                  没有账号？直接建档
                </Link>
              </div>
            </MarketingReveal>
          </main>
        </div>
      </div>
    </div>
  );
}
