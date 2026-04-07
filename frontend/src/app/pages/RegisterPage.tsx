import React, { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router';

import { MarketingFloat, MarketingReveal, MarketingStagger, MarketingStaggerItem } from '../components/MarketingMotion';
import { DashboardInput } from '../components/ui/dashboard-input';
import { DashboardFieldLabel } from '../components/ui/dashboard-select';
import { DashboardSegmentedGroup, DashboardSegmentedItem } from '../components/ui/dashboard-segmented';
import { useStudent } from '../../hooks/useStudent';
import type { StudentCreate } from '../../lib/types';
import {
  buildWorkspacePath,
  createSessionId,
  persistWorkspaceIdentity,
  readWorkspaceSnapshot,
} from '../../lib/workspaceSession';

const CURRICULUM_OPTIONS = ['AP', 'IB', 'A-Level', 'Other'] as const;
const GPA_SCALE_OPTIONS = ['4.0', '100', '5.0'] as const;
const TARGET_YEAR_OPTIONS = ['2026', '2027', '2028', '2029'] as const;
const MAJOR_PRESETS = ['计算机科学', '经济学', '商科', '生物/Pre-Med', '工程', '社会科学'] as const;

function FormField({
  label,
  hint,
  htmlFor,
  children,
}: {
  label: string;
  hint?: string;
  htmlFor?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2">
      <DashboardFieldLabel htmlFor={htmlFor} className="text-[10px] tracking-[0.14em] text-[#17304b]/62">
        {label}
      </DashboardFieldLabel>
      {children}
      {hint ? <p className="text-xs leading-6 text-[#17304b]/54">{hint}</p> : null}
    </div>
  );
}

export function RegisterPage() {
  const navigate = useNavigate();
  const { createStudent, isLoading, error } = useStudent();
  const [form, setForm] = useState({
    name: '',
    gpa: '3.8',
    gpaScale: '4.0',
    satTotal: '',
    major: '计算机科学',
    targetYear: '2027',
    curriculumType: 'AP',
    budgetUsd: '70000',
    needFinancialAid: 'yes',
  });

  const resumePath = useMemo(() => {
    const snapshot = readWorkspaceSnapshot();
    return snapshot.studentId ? buildWorkspacePath(snapshot.sessionId, 'advisor') : null;
  }, []);

  useEffect(() => {
    document.title = '开始建档 | ScholarPath';
    document.documentElement.lang = 'zh-CN';
  }, []);

  const canSubmit =
    form.name.trim().length > 0 &&
    form.major.trim().length > 0 &&
    Number(form.gpa) >= 0 &&
    Number(form.targetYear) >= 2024;

  async function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSubmit || isLoading) return;

    const payload: StudentCreate = {
      name: form.name.trim(),
      gpa: Number(form.gpa),
      gpa_scale: form.gpaScale,
      sat_total: form.satTotal ? Number(form.satTotal) : null,
      act_composite: null,
      toefl_total: null,
      curriculum_type: form.curriculumType,
      ap_courses: [],
      extracurriculars: [],
      awards: [],
      intended_majors: [form.major.trim()],
      budget_usd: form.budgetUsd ? Number(form.budgetUsd) : null,
      need_financial_aid: form.needFinancialAid === 'yes',
      preferences: {
        ui_preference_tags: ['landing-signup'],
      },
      ed_preference: null,
      target_year: Number(form.targetYear),
    };

    const created = await createStudent(payload);
    if (!created) return;

    const sessionId = createSessionId();
    persistWorkspaceIdentity({
      studentId: created.id,
      studentName: created.name,
      sessionId,
      locale: 'zh',
    });
    navigate(buildWorkspacePath(sessionId, 'advisor'));
  }

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
                <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-[#17304b]/52">进入你的申请工作台</div>
              </div>
            </Link>

            <div className="flex items-center gap-3">
              {resumePath && (
                <Link
                  to={resumePath}
                  className="hidden rounded-full border border-[#17304b]/10 bg-white/78 px-4 py-2 text-sm font-bold text-[#17304b] shadow-[0_12px_28px_rgba(15,23,42,0.06)] backdrop-blur md:inline-flex"
                >
                  继续已有 workspace
                </Link>
              )}
              <Link
                to="/"
                className="inline-flex items-center gap-2 rounded-full bg-white/76 px-4 py-2 text-sm font-bold text-[#17304b] shadow-[0_12px_28px_rgba(15,23,42,0.06)] backdrop-blur"
              >
                <span className="material-symbols-outlined text-[18px]">west</span>
                返回介绍页
              </Link>
            </div>
          </header>

          <main className="grid flex-1 gap-8 py-8 lg:grid-cols-[minmax(0,0.68fr)_minmax(420px,0.96fr)] lg:items-center">
            <MarketingStagger mode="immediate" className="space-y-7" delay={0.04} stagger={0.08}>
              <MarketingStaggerItem>
                <div className="inline-flex items-center gap-2 rounded-full border border-[#17304b]/10 bg-white/74 px-4 py-2 text-[11px] font-bold uppercase tracking-[0.18em] text-[#17304b] shadow-[0_12px_24px_rgba(15,23,42,0.06)] backdrop-blur">
                  <span className="material-symbols-outlined text-[16px]">article</span>
                  1 分钟完成首版档案
                </div>
              </MarketingStaggerItem>

              <MarketingStaggerItem className="space-y-4">
                <h1 className="font-headline text-5xl font-black leading-[0.95] tracking-[-0.05em] text-[#10253d] sm:text-6xl">
                  先把你的约束写清楚，
                  <span className="block text-[#17304b]">下一步就直接进入 workspace。</span>
                </h1>
                <p className="max-w-xl text-base leading-8 text-[#17304b]/74">
                  这里只收集 Advisor 真正需要的基础信息。提交之后不会停在空白页，而是直接进入推荐、筛校和决策界面。
                </p>
              </MarketingStaggerItem>

              <MarketingStaggerItem>
                <div className="overflow-hidden rounded-[2rem] border border-white/72 bg-[linear-gradient(180deg,rgba(255,255,255,0.95),rgba(244,238,228,0.9))] p-5 shadow-[0_24px_58px_rgba(15,23,42,0.08)]">
                  <div className="text-[11px] font-bold uppercase tracking-[0.18em] text-[#17304b]/50">进入后会得到</div>
                  <div className="mt-4 grid gap-3 sm:grid-cols-2">
                    {[
                      {
                        title: '首轮结构',
                        body: '直接看到 Reach / Match / Safety 的第一版框架。',
                      },
                      {
                        title: '继续决策',
                        body: '后续能无缝进入 offer compare 与择校分析。',
                      },
                    ].map((item) => (
                      <div
                        key={item.title}
                        className="rounded-[1.35rem] border border-[#17304b]/8 bg-white/84 px-4 py-4 shadow-[0_12px_28px_rgba(15,23,42,0.04)]"
                      >
                        <div className="text-xs font-black uppercase tracking-[0.14em] text-[#17304b]/54">{item.title}</div>
                        <div className="mt-2 text-sm leading-7 text-[#17304b]/78">{item.body}</div>
                      </div>
                    ))}
                  </div>
                </div>
              </MarketingStaggerItem>
            </MarketingStagger>

            <MarketingReveal
              mode="immediate"
              amount={26}
              scale={0.988}
              className="rounded-[2.15rem] border border-white/78 bg-[linear-gradient(180deg,rgba(255,255,255,0.96),rgba(244,238,228,0.9))] p-6 shadow-[0_30px_72px_rgba(15,23,42,0.1)] sm:p-8"
            >
              <div className="mb-6">
                <div className="text-[11px] font-bold uppercase tracking-[0.18em] text-[#17304b]/54">创建档案</div>
                <h2 className="mt-3 font-headline text-3xl font-black tracking-tight text-[#10253d]">创建你的首版申请档案</h2>
                <p className="mt-2 text-sm leading-7 text-[#17304b]/66">
                  现在只填最关键的信息。更多活动、奖项和偏好，可以进 workspace 后再继续补。
                </p>
              </div>

              <form className="space-y-5" onSubmit={handleSubmit}>
                <MarketingStagger mode="immediate" className="space-y-5" delay={0.04} stagger={0.05}>
                  <MarketingStaggerItem>
                    <div className="grid gap-5 sm:grid-cols-2">
                      <FormField label="姓名" htmlFor="register-name">
                        <DashboardInput
                          id="register-name"
                          value={form.name}
                          onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))}
                          placeholder="例如：Luna Chen"
                        />
                      </FormField>

                      <FormField label="目标入学年份">
                        <DashboardSegmentedGroup
                          type="single"
                          value={form.targetYear}
                          onValueChange={(value) => value && setForm((prev) => ({ ...prev, targetYear: value }))}
                          className="grid grid-cols-2 gap-2"
                          size="compact"
                          accent="marketing"
                        >
                          {TARGET_YEAR_OPTIONS.map((year) => (
                            <DashboardSegmentedItem key={year} value={year} className="justify-center">
                              {year}
                            </DashboardSegmentedItem>
                          ))}
                        </DashboardSegmentedGroup>
                      </FormField>
                    </div>
                  </MarketingStaggerItem>

                  <MarketingStaggerItem>
                    <div className="grid gap-5 sm:grid-cols-2">
                      <FormField label="GPA" htmlFor="register-gpa">
                        <DashboardInput
                          id="register-gpa"
                          type="number"
                          step="0.01"
                          value={form.gpa}
                          onChange={(event) => setForm((prev) => ({ ...prev, gpa: event.target.value }))}
                          placeholder="例如：3.85"
                        />
                      </FormField>

                      <FormField label="GPA 制式">
                        <DashboardSegmentedGroup
                          type="single"
                          value={form.gpaScale}
                          onValueChange={(value) => value && setForm((prev) => ({ ...prev, gpaScale: value }))}
                          className="grid grid-cols-3 gap-2"
                          size="compact"
                          accent="marketing"
                        >
                          {GPA_SCALE_OPTIONS.map((scale) => (
                            <DashboardSegmentedItem key={scale} value={scale} className="justify-center">
                              {scale}
                            </DashboardSegmentedItem>
                          ))}
                        </DashboardSegmentedGroup>
                      </FormField>
                    </div>
                  </MarketingStaggerItem>

                  <MarketingStaggerItem>
                    <FormField label="意向专业" htmlFor="register-major">
                      <div className="space-y-3">
                        <DashboardInput
                          id="register-major"
                          value={form.major}
                          onChange={(event) => setForm((prev) => ({ ...prev, major: event.target.value }))}
                          placeholder="例如：计算机科学"
                        />
                        <div className="flex flex-wrap gap-1.5 sm:gap-2">
                          {MAJOR_PRESETS.map((major) => (
                            <button
                              key={major}
                              type="button"
                              onClick={() => setForm((prev) => ({ ...prev, major }))}
                              className={`rounded-full px-3 py-1.5 text-xs font-bold transition ${
                                form.major === major
                                  ? 'bg-[linear-gradient(135deg,#17304b,#0f2237)] text-[#fff6e9] shadow-[0_14px_30px_rgba(12,27,45,0.18)]'
                                  : 'border border-[#17304b]/10 bg-[linear-gradient(180deg,rgba(255,251,245,0.98),rgba(244,235,221,0.94))] text-[#17304b] shadow-[0_8px_18px_rgba(15,23,42,0.04)] hover:-translate-y-0.5 hover:bg-white'
                              }`}
                            >
                              {major}
                            </button>
                          ))}
                        </div>
                      </div>
                    </FormField>
                  </MarketingStaggerItem>

                  <MarketingStaggerItem>
                    <div className="grid gap-5 sm:grid-cols-2">
                      <FormField label="课程体系">
                        <DashboardSegmentedGroup
                          type="single"
                          value={form.curriculumType}
                          onValueChange={(value) => value && setForm((prev) => ({ ...prev, curriculumType: value }))}
                          className="grid grid-cols-2 gap-2"
                          size="compact"
                          accent="marketing"
                        >
                          {CURRICULUM_OPTIONS.map((option) => (
                            <DashboardSegmentedItem key={option} value={option} className="justify-center">
                              {option}
                            </DashboardSegmentedItem>
                          ))}
                        </DashboardSegmentedGroup>
                      </FormField>

                      <FormField label="SAT（可选）" hint="如果还没有成绩，可以先留空。" htmlFor="register-sat">
                        <DashboardInput
                          id="register-sat"
                          type="number"
                          value={form.satTotal}
                          onChange={(event) => setForm((prev) => ({ ...prev, satTotal: event.target.value }))}
                          placeholder="例如：1520"
                        />
                      </FormField>
                    </div>
                  </MarketingStaggerItem>

                  <MarketingStaggerItem>
                    <div className="grid gap-5 sm:grid-cols-2">
                      <FormField label="年度预算（USD）" htmlFor="register-budget">
                        <DashboardInput
                          id="register-budget"
                          type="number"
                          value={form.budgetUsd}
                          onChange={(event) => setForm((prev) => ({ ...prev, budgetUsd: event.target.value }))}
                          placeholder="例如：70000"
                        />
                      </FormField>

                      <FormField label="是否需要奖助学金">
                        <DashboardSegmentedGroup
                          type="single"
                          value={form.needFinancialAid}
                          onValueChange={(value) => value && setForm((prev) => ({ ...prev, needFinancialAid: value }))}
                          className="grid grid-cols-2 gap-2"
                          size="compact"
                          accent="marketing"
                        >
                          <DashboardSegmentedItem value="yes" className="justify-center">
                            需要
                          </DashboardSegmentedItem>
                          <DashboardSegmentedItem value="no" className="justify-center">
                            暂不需要
                          </DashboardSegmentedItem>
                        </DashboardSegmentedGroup>
                      </FormField>
                    </div>
                  </MarketingStaggerItem>
                </MarketingStagger>

                {error && (
                  <div className="rounded-2xl border border-error/15 bg-error/6 px-4 py-3 text-sm text-error">
                    建档失败：{error.message}
                  </div>
                )}

                <div className="flex flex-col gap-3 pt-2 sm:flex-row sm:items-center sm:justify-between">
                  <p className="max-w-md text-xs leading-6 text-[#17304b]/62">
                    提交后会直接进入你的 ScholarPath workspace，并自动保存这份学生档案。
                  </p>
                  <button
                    type="submit"
                    disabled={!canSubmit || isLoading}
                    className="inline-flex items-center justify-center gap-2 rounded-full bg-[linear-gradient(135deg,#17304b,#0f2237)] px-6 py-4 text-sm font-black text-white shadow-[0_20px_44px_rgba(12,27,45,0.24)] transition hover:-translate-y-0.5 hover:brightness-110 disabled:translate-y-0 disabled:cursor-not-allowed disabled:opacity-55"
                  >
                    {isLoading ? '正在创建并进入 workspace…' : '创建档案并进入 workspace'}
                    <span className="material-symbols-outlined text-[18px]">arrow_forward</span>
                  </button>
                </div>
              </form>
            </MarketingReveal>
          </main>
        </div>
      </div>
    </div>
  );
}
