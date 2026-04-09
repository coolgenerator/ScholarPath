import React, { useEffect, useState } from 'react';
import { Link } from 'react-router';

import { MarketingFloat, MarketingReveal, MarketingStagger, MarketingStaggerItem } from '../components/MarketingMotion';
import { buildWorkspacePath, readWorkspaceSnapshot } from '../../lib/workspaceSession';

const SHOTS = {
  schoolListDesktop: '/output/ui-shots/landing-school-list-desktop.png',
  schoolListMobile: '/output/ui-shots/landing-school-list-mobile.png',
  offersDesktop: '/output/ui-shots/landing-offers-desktop.png',
  offersMobile: '/output/ui-shots/landing-offers-mobile.png',
  decisionsDesktop: '/output/ui-shots/landing-decisions-desktop.png',
  decisionsMobile: '/output/ui-shots/landing-decisions-mobile.png',
} as const;

type ScreenshotStatus = 'loading' | 'ready' | 'missing';

function FrameLabel({ children }: { children: React.ReactNode }) {
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-[#17304b]/10 bg-white/72 px-3 py-1.5 text-[10px] font-bold uppercase tracking-[0.18em] text-[#17304b]/74 shadow-[0_12px_24px_rgba(12,27,45,0.08)] backdrop-blur">
      {children}
    </div>
  );
}

function NarrativeBubble({
  role,
  text,
  dark = false,
}: {
  role: string;
  text: string;
  dark?: boolean;
}) {
  return (
    <div
      className={`max-w-[21rem] rounded-[1.6rem] border px-4 py-3 shadow-[0_22px_46px_rgba(18,31,48,0.16)] backdrop-blur-xl ${
        dark
          ? 'border-[#17304b]/12 bg-[#17304b]/92 text-white'
          : 'border-white/72 bg-white/90 text-[#132741]'
      }`}
    >
      <div className={`text-[10px] font-bold uppercase tracking-[0.18em] ${dark ? 'text-white/56' : 'text-[#17304b]/46'}`}>{role}</div>
      <p className={`mt-2 text-sm leading-7 ${dark ? 'text-white/84' : 'text-[#17304b]/78'}`}>{text}</p>
    </div>
  );
}

function ScreenshotCard({
  src,
  alt,
  chromeLabel,
  className = '',
  minHeightClass = 'min-h-[18rem]',
}: {
  src: string;
  alt: string;
  chromeLabel: string;
  className?: string;
  minHeightClass?: string;
}) {
  const [status, setStatus] = useState<ScreenshotStatus>('loading');

  useEffect(() => {
    let cancelled = false;
    const image = new window.Image();

    setStatus('loading');
    image.onload = () => {
      if (!cancelled) {
        setStatus('ready');
      }
    };
    image.onerror = () => {
      if (!cancelled) {
        setStatus('missing');
      }
    };
    image.src = src;

    return () => {
      cancelled = true;
    };
  }, [src]);

  return (
    <div
      className={`overflow-hidden rounded-[2rem] border border-white/70 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(244,238,228,0.92))] p-3 shadow-[0_34px_80px_rgba(17,28,45,0.18)] backdrop-blur ${className}`}
    >
      <div className="mb-3 flex items-center justify-between rounded-[1.1rem] border border-[#17304b]/8 bg-[#f6f0e6]/92 px-4 py-2">
        <div className="flex items-center gap-2">
          <span className="h-2.5 w-2.5 rounded-full bg-[#d09b52]" />
          <span className="h-2.5 w-2.5 rounded-full bg-[#a67f49]/70" />
          <span className="h-2.5 w-2.5 rounded-full bg-[#17304b]/70" />
        </div>
        <span className="text-[10px] font-bold uppercase tracking-[0.16em] text-[#17304b]/58">{chromeLabel}</span>
      </div>

      {status === 'missing' ? (
        <div className={`flex ${minHeightClass} items-center justify-center rounded-[1.3rem] border border-dashed border-[#17304b]/12 bg-[radial-gradient(circle_at_top,rgba(208,155,82,0.15),transparent_40%),linear-gradient(180deg,#f6efe4,#ece3d5)] p-8 text-center`}>
          <div>
            <div className="font-headline text-2xl font-black text-[#17304b]">最新 UI 素材待生成</div>
            <p className="mt-3 text-sm leading-7 text-[#17304b]/68">
              运行 <span className="font-bold">npm run capture:landing-shots</span> 后，这里会自动换成当前本地 UI 截图。
            </p>
          </div>
        </div>
      ) : status === 'ready' ? (
        <img
          src={src}
          alt={alt}
          loading="eager"
          className="block h-auto w-full rounded-[1.35rem] border border-[#17304b]/6 object-cover"
        />
      ) : (
        <div className={`flex ${minHeightClass} animate-pulse items-center justify-center rounded-[1.3rem] border border-[#17304b]/8 bg-[linear-gradient(135deg,rgba(255,255,255,0.92),rgba(241,233,221,0.92))]`}>
          <div className="space-y-3 text-center">
            <div className="mx-auto h-2.5 w-24 rounded-full bg-[#17304b]/12" />
            <div className="mx-auto h-28 w-64 max-w-[76vw] rounded-[1.2rem] bg-[#17304b]/8" />
          </div>
        </div>
      )}
    </div>
  );
}

function SectionCopy({
  label,
  title,
  body,
}: {
  label: string;
  title: string;
  body: string;
}) {
  return (
    <div className="space-y-5">
      <FrameLabel>{label}</FrameLabel>
      <div className="space-y-4">
        <h2 className="max-w-xl font-headline text-4xl font-black leading-[0.98] tracking-[-0.04em] text-[#10253d] sm:text-5xl">
          {title}
        </h2>
        <p className="max-w-xl text-base leading-8 text-[#17304b]/72">{body}</p>
      </div>
    </div>
  );
}

export function LandingPage() {
  const snapshot = readWorkspaceSnapshot();
  const resumePath = snapshot.studentId ? buildWorkspacePath(snapshot.sessionId, 'advisor') : null;

  useEffect(() => {
    document.title = 'ScholarPath | AI 留学择校工作台';
    document.documentElement.lang = 'zh-CN';
  }, []);

  return (
    <div className="min-h-screen overflow-x-hidden bg-[radial-gradient(circle_at_top,rgba(23,48,75,0.16),transparent_26%),radial-gradient(circle_at_86%_12%,rgba(208,155,82,0.22),transparent_22%),linear-gradient(180deg,#f4eee4_0%,#ebe1cf_48%,#f8f3eb_100%)] text-[#10253d]">
      <div className="relative isolate">
        <MarketingFloat className="pointer-events-none absolute -left-24 top-20 hidden lg:block" y={18} x={10} duration={16}>
          <div className="h-72 w-72 rounded-full bg-[#17304b]/10 blur-3xl" />
        </MarketingFloat>
        <MarketingFloat className="pointer-events-none absolute right-0 top-10" y={14} x={-10} duration={12}>
          <div className="h-56 w-56 rounded-full bg-[#d09b52]/16 blur-3xl" />
        </MarketingFloat>
        <MarketingFloat className="pointer-events-none absolute inset-x-0 top-0" y={8} duration={14}>
          <div className="h-[38rem] bg-[linear-gradient(180deg,rgba(10,21,37,0.08),transparent)]" />
        </MarketingFloat>

        <header className="mx-auto flex w-full max-w-7xl items-center justify-between px-6 py-6 lg:px-10">
          <Link to="/" className="flex items-center gap-4">
            <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-[#17304b] text-white shadow-[0_18px_40px_rgba(12,27,45,0.22)]">
              <span className="material-symbols-outlined text-[25px]">school</span>
            </div>
            <div>
              <div className="font-headline text-xl font-black tracking-tight text-[#10253d]">ScholarPath</div>
              <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-[#17304b]/52">AI 留学决策工作台</div>
            </div>
          </Link>

          <div className="flex items-center gap-3">
            {resumePath && (
              <Link
                to={resumePath}
                className="hidden rounded-full border border-[#17304b]/10 bg-white/74 px-4 py-2 text-sm font-bold text-[#17304b] shadow-[0_12px_28px_rgba(15,23,42,0.08)] backdrop-blur md:inline-flex"
              >
                继续我的 workspace
              </Link>
            )}
            <Link
              to="/login"
              className="inline-flex items-center gap-2 rounded-full bg-[#17304b] px-5 py-2.5 text-sm font-bold text-white shadow-[0_18px_36px_rgba(12,27,45,0.24)] transition hover:-translate-y-0.5 hover:bg-[#0f2237]"
            >
              登录 / 注册
              <span className="material-symbols-outlined text-[18px]">north_east</span>
            </Link>
          </div>
        </header>

        <main className="mx-auto flex w-full max-w-7xl flex-col gap-18 px-6 pb-16 pt-4 sm:gap-22 lg:gap-24 lg:px-10 lg:pb-24">
          <section className="grid gap-10 lg:grid-cols-[minmax(0,0.92fr)_minmax(0,1.08fr)] lg:items-center">
            <MarketingStagger mode="immediate" className="space-y-7" delay={0.04} stagger={0.08}>
              <MarketingStaggerItem>
                <FrameLabel>真实 UI 驱动的申请叙事</FrameLabel>
              </MarketingStaggerItem>

              <MarketingStaggerItem className="space-y-5">
                <h1 className="max-w-3xl font-headline text-5xl font-black leading-[0.92] tracking-[-0.05em] text-[#10253d] sm:text-6xl lg:text-7xl">
                  把留学申请里最难的判断，
                  <span className="block text-[#17304b]">收进一个更清楚的工作台。</span>
                </h1>
                <p className="max-w-xl text-base leading-8 text-[#17304b]/74 sm:text-lg">
                  从第一版选校，到录取比较，再到偏好化决策，ScholarPath 用一条连续界面把噪音压缩成可以执行的判断。
                </p>
              </MarketingStaggerItem>

              <MarketingStaggerItem className="flex flex-col gap-3 sm:flex-row">
                <Link
                  to="/login"
                  className="inline-flex items-center justify-center gap-2 rounded-full bg-[linear-gradient(135deg,#17304b,#0f2237)] px-6 py-4 text-sm font-black text-white shadow-[0_24px_54px_rgba(12,27,45,0.26)] transition hover:-translate-y-0.5 hover:brightness-110"
                >
                  登录并进入 workspace
                  <span className="material-symbols-outlined text-[18px]">arrow_forward</span>
                </Link>
                {resumePath ? (
                  <Link
                    to={resumePath}
                    className="inline-flex items-center justify-center rounded-full border border-[#17304b]/10 bg-white/78 px-6 py-4 text-sm font-bold text-[#17304b] shadow-[0_16px_36px_rgba(15,23,42,0.08)] backdrop-blur transition hover:-translate-y-0.5"
                  >
                    继续已有 workspace
                  </Link>
                ) : (
                  <Link
                    to="/register"
                    className="inline-flex items-center justify-center rounded-full border border-[#17304b]/10 bg-white/78 px-6 py-4 text-sm font-bold text-[#17304b] shadow-[0_16px_36px_rgba(15,23,42,0.08)] backdrop-blur transition hover:-translate-y-0.5"
                  >
                    直接建档
                  </Link>
                )}
              </MarketingStaggerItem>

              <MarketingStaggerItem>
                <div className="text-[11px] font-bold uppercase tracking-[0.16em] text-[#17304b]/46">
                  最新素材来自本地真实 UI 自动采集，不再是旧静态图。
                </div>
              </MarketingStaggerItem>
            </MarketingStagger>

            <MarketingReveal mode="immediate" className="relative lg:min-h-[40rem]" amount={34} scale={0.98}>
              <div className="lg:hidden">
                <ScreenshotCard
                  src={SHOTS.schoolListMobile}
                  alt="ScholarPath 最新选校列表移动端界面"
                  chromeLabel="最新 UI 主视觉"
                  minHeightClass="min-h-[22rem]"
                />
              </div>

              <div className="hidden lg:block">
                <MarketingFloat className="relative z-[2] ml-auto w-full max-w-[54rem]" y={10} rotate={0.8} duration={13}>
                  <ScreenshotCard
                    src={SHOTS.schoolListDesktop}
                    alt="ScholarPath 最新选校列表界面"
                    chromeLabel="最新 UI 合成主视觉"
                    minHeightClass="min-h-[28rem]"
                  />
                </MarketingFloat>

                <MarketingFloat className="absolute -bottom-2 left-0 z-[3] w-[16rem]" y={14} rotate={-1.4} duration={11.5} delay={0.4}>
                  <ScreenshotCard
                    src={SHOTS.offersMobile}
                    alt="ScholarPath 最新录取比较移动端界面"
                    chromeLabel="移动端"
                    minHeightClass="min-h-[14rem]"
                  />
                </MarketingFloat>

                <MarketingFloat className="absolute right-4 top-10 z-[3] w-[15rem]" y={12} rotate={1.1} duration={12.8} delay={0.7}>
                  <ScreenshotCard
                    src={SHOTS.decisionsMobile}
                    alt="ScholarPath 最新智能择校移动端界面"
                    chromeLabel="决策面板"
                    minHeightClass="min-h-[14rem]"
                  />
                </MarketingFloat>
              </div>
            </MarketingReveal>
          </section>

          <section className="grid gap-8 lg:grid-cols-[minmax(0,0.42fr)_minmax(0,0.58fr)] lg:items-center">
            <MarketingReveal>
              <SectionCopy
                label="选校证明"
                title="先看到真实推荐，再决定怎么收敛。"
                body="这里直接展示当前工作台里的真实选校界面。模拟对话只保留最小叙事，主证据仍然是产品本身。"
              />
            </MarketingReveal>

            <MarketingReveal className="relative space-y-4 lg:min-h-[36rem]" amount={30} scale={0.985}>
              <div className="lg:hidden">
                <ScreenshotCard
                  src={SHOTS.schoolListMobile}
                  alt="ScholarPath 真实选校移动端截图"
                  chromeLabel="最新真实选校界面"
                  minHeightClass="min-h-[21rem]"
                />
              </div>

              <div className="hidden lg:block">
                <MarketingFloat className="relative z-[2]" y={10} rotate={0.6} duration={13.2}>
                  <ScreenshotCard
                    src={SHOTS.schoolListDesktop}
                    alt="ScholarPath 真实选校列表截图"
                    chromeLabel="最新真实选校界面"
                    minHeightClass="min-h-[26rem]"
                  />
                </MarketingFloat>

                <MarketingFloat className="absolute -bottom-2 right-4 z-[3] w-[13rem]" y={14} rotate={1.2} duration={11.8} delay={0.5}>
                  <ScreenshotCard
                    src={SHOTS.schoolListMobile}
                    alt="ScholarPath 真实选校移动端截图"
                    chromeLabel="移动端"
                    minHeightClass="min-h-[14rem]"
                  />
                </MarketingFloat>
              </div>

              <MarketingStagger className="mt-4 flex flex-col gap-3 lg:hidden" delay={0.04}>
                <MarketingStaggerItem amount={14}>
                  <NarrativeBubble
                    role="学生输入"
                    text="我是国际学校学生，GPA 3.86，SAT 1510，预算 7 万美元/年，想申 CS，希望实习和奖学金都不错。"
                  />
                </MarketingStaggerItem>
                <MarketingStaggerItem amount={14}>
                  <NarrativeBubble
                    role="Agent 输出"
                    text="先给你一版冲刺/匹配/保底结构，再按预算和奖学金敏感度继续收敛。"
                    dark
                  />
                </MarketingStaggerItem>
              </MarketingStagger>

              <div className="hidden lg:block">
                <MarketingReveal className="absolute -left-3 top-10 z-[4]" amount={16} blur>
                  <NarrativeBubble
                    role="学生输入"
                    text="我是国际学校学生，GPA 3.86，SAT 1510，预算 7 万美元/年，想申 CS，希望实习和奖学金都不错。"
                  />
                </MarketingReveal>
                <MarketingReveal className="absolute bottom-14 left-8 z-[4]" delay={0.08} amount={16} blur>
                  <NarrativeBubble
                    role="Agent 输出"
                    text="先给你一版冲刺/匹配/保底结构，再按预算和奖学金敏感度继续收敛。"
                    dark
                  />
                </MarketingReveal>
              </div>
            </MarketingReveal>
          </section>

          <section className="grid gap-8 lg:grid-cols-[minmax(0,0.6fr)_minmax(0,0.4fr)] lg:items-center">
            <MarketingReveal className="relative space-y-4 lg:min-h-[40rem]" amount={30} scale={0.985}>
              <div className="space-y-4 lg:hidden">
                <ScreenshotCard
                  src={SHOTS.offersMobile}
                  alt="ScholarPath 真实录取比较移动端截图"
                  chromeLabel="录取比较"
                  minHeightClass="min-h-[21rem]"
                />
                <div className="mx-auto max-w-[18rem]">
                  <ScreenshotCard
                    src={SHOTS.decisionsMobile}
                    alt="ScholarPath 真实智能择校移动端截图"
                    chromeLabel="智能择校"
                    minHeightClass="min-h-[14rem]"
                  />
                </div>
              </div>

              <div className="hidden lg:block">
                <MarketingFloat className="relative z-[2] mr-auto w-full max-w-[46rem]" y={10} rotate={-0.4} duration={12.6}>
                  <ScreenshotCard
                    src={SHOTS.offersDesktop}
                    alt="ScholarPath 真实录取比较截图"
                    chromeLabel="录取比较"
                    minHeightClass="min-h-[28rem]"
                  />
                </MarketingFloat>

                <MarketingFloat className="absolute bottom-0 right-0 z-[3] w-[65%]" y={12} rotate={1.2} duration={13.4} delay={0.4}>
                  <ScreenshotCard
                    src={SHOTS.decisionsDesktop}
                    alt="ScholarPath 真实智能择校截图"
                    chromeLabel="智能择校"
                    minHeightClass="min-h-[18rem]"
                  />
                </MarketingFloat>

                <MarketingFloat className="absolute right-8 top-2 z-[4] w-[13rem]" y={11} rotate={1.4} duration={11.9} delay={0.8}>
                  <ScreenshotCard
                    src={SHOTS.decisionsMobile}
                    alt="ScholarPath 真实智能择校移动端截图"
                    chromeLabel="移动端"
                    minHeightClass="min-h-[14rem]"
                  />
                </MarketingFloat>
              </div>

              <MarketingStagger className="mt-4 flex flex-col gap-3 lg:hidden" delay={0.04}>
                <MarketingStaggerItem amount={14}>
                  <NarrativeBubble role="学生输入" text="UIUC 和 Purdue 都录了，预算和长期回报怎么取舍？" />
                </MarketingStaggerItem>
                <MarketingStaggerItem amount={14}>
                  <NarrativeBubble
                    role="Agent 输出"
                    text="先比较净花费与 aid，再看职业回报、风险和个人偏好。"
                    dark
                  />
                </MarketingStaggerItem>
              </MarketingStagger>

              <div className="hidden lg:block">
                <MarketingReveal className="absolute left-4 top-6 z-[4]" amount={16}>
                  <NarrativeBubble role="学生输入" text="UIUC 和 Purdue 都录了，预算和长期回报怎么取舍？" />
                </MarketingReveal>
                <MarketingReveal className="absolute bottom-8 left-10 z-[4]" delay={0.08} amount={16}>
                  <NarrativeBubble
                    role="Agent 输出"
                    text="先比较净花费与 aid，再看职业回报、风险和个人偏好。"
                    dark
                  />
                </MarketingReveal>
              </div>
            </MarketingReveal>

            <MarketingReveal>
              <SectionCopy
                label="决策证明"
                title="推荐不是终点，录取之后才是真正的取舍。"
                body="ScholarPath 把 offer compare 和决策权重面板接在一起，让名单继续延伸成净花费、回报和偏好的比较。"
              />
            </MarketingReveal>
          </section>

          <MarketingReveal
            className="overflow-hidden rounded-[2.5rem] border border-white/72 bg-[linear-gradient(135deg,rgba(16,37,61,0.96),rgba(22,48,75,0.92),rgba(125,88,34,0.78))] px-8 py-10 shadow-[0_34px_90px_rgba(10,20,36,0.22)] sm:px-10 sm:py-12"
            viewportAmount={0.2}
          >
            <div className="grid gap-8 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
              <div className="space-y-4">
                <div className="text-[11px] font-bold uppercase tracking-[0.18em] text-white/62">Closing CTA</div>
                <h2 className="max-w-3xl font-headline text-4xl font-black leading-[0.98] tracking-[-0.04em] text-white sm:text-5xl">
                  先建立你的学生档案，
                  <span className="block text-[#f0d7a8]">下一步就直接进入真实 workspace。</span>
                </h2>
                <p className="max-w-2xl text-base leading-8 text-white/76">
                  不多一步，不讲空话。把背景、预算和专业方向填进去，ScholarPath 会从建档页直接把你送进工作台。
                </p>
              </div>

              <MarketingStagger className="flex flex-col gap-3 sm:flex-row lg:flex-col" delay={0.06} mode="view">
                <MarketingStaggerItem amount={12}>
                  <Link
                    to="/login"
                    className="inline-flex items-center justify-center gap-2 rounded-full bg-white px-6 py-4 text-sm font-black text-[#17304b] shadow-[0_18px_40px_rgba(8,17,28,0.18)] transition hover:-translate-y-0.5"
                  >
                    登录 / 注册
                    <span className="material-symbols-outlined text-[18px]">arrow_forward</span>
                  </Link>
                </MarketingStaggerItem>
                {resumePath && (
                  <MarketingStaggerItem amount={12}>
                    <Link
                      to={resumePath}
                      className="inline-flex items-center justify-center rounded-full border border-white/20 bg-white/10 px-6 py-4 text-sm font-bold text-white backdrop-blur transition hover:bg-white/14"
                    >
                      继续已有 workspace
                    </Link>
                  </MarketingStaggerItem>
                )}
              </MarketingStagger>
            </div>
          </MarketingReveal>
        </main>
      </div>
    </div>
  );
}
