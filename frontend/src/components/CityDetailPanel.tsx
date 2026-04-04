'use client';

import { useState } from 'react';
import {
  X, Users, DollarSign, Building2, Shield, BarChart3,
  Brain, MapPin, Loader2, ChevronDown, ChevronUp, Zap,
} from 'lucide-react';
import { api } from '@/lib/api';
import type { MunicipioDetail, TradeAreaItem, TradeAreaInsightsRequest } from '@/lib/types';
import { tierColor, scoreToColor } from '@/lib/colors';

interface Props {
  detail: MunicipioDetail;
  onClose: () => void;
  onTradeAreaLoaded: (center: [number, number], items: TradeAreaItem[]) => void;
}

// ── Small helpers ─────────────────────────────────────────────────────────────
function fmt(v: number | null | undefined, prefix = '', decimals = 0): string {
  if (v === null || v === undefined) return '—';
  return prefix + v.toLocaleString('pt-BR', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function pct(v: number | null | undefined): string {
  if (v === null || v === undefined) return '—';
  return v.toFixed(1) + '%';
}

// ── Lightweight Markdown renderer (no external deps) ──────────────────────────
// Handles the subset Claude generates: ##/### headings, **bold**, - bullets, ---

function renderInline(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith('**') && part.endsWith('**')
      ? <strong key={i} className="text-white font-semibold">{part.slice(2, -2)}</strong>
      : part,
  );
}

function MarkdownBlock({ text }: { text: string }) {
  const lines = text.split('\n');
  return (
    <div className="space-y-0.5">
      {lines.map((line, i) => {
        if (line.startsWith('### '))
          return <h4 key={i} className="text-[11px] font-bold text-white mt-3 mb-0.5">{line.replace(/^###\s+/, '')}</h4>;
        if (line.startsWith('## '))
          return <h3 key={i} className="text-xs font-bold text-[var(--primary)] mt-3 mb-0.5">{line.replace(/^##\s+/, '')}</h3>;
        if (line === '---')
          return <hr key={i} className="border-[var(--border)] my-2" />;
        if (/^[-*]\s/.test(line))
          return (
            <div key={i} className="flex gap-1.5 ml-2">
              <span className="text-[var(--muted)] flex-shrink-0 select-none">·</span>
              <span>{renderInline(line.replace(/^[-*]\s+/, ''))}</span>
            </div>
          );
        if (line.trim() === '')
          return <div key={i} className="h-1.5" />;
        return <p key={i}>{renderInline(line)}</p>;
      })}
    </div>
  );
}

// ── Stat card ─────────────────────────────────────────────────────────────────
function StatCard({
  icon: Icon, label, value, sub,
}: {
  icon: React.ElementType; label: string; value: string; sub?: string;
}) {
  return (
    <div className="glass p-3">
      <div className="flex items-center gap-1.5 text-[var(--muted)] text-[10px] mb-1">
        <Icon size={11} /> {label}
      </div>
      <div className="text-base font-bold text-white">{value}</div>
      {sub && <div className="text-[10px] text-[var(--text-dim)]">{sub}</div>}
    </div>
  );
}

// ── Score bar ─────────────────────────────────────────────────────────────────
function ScoreRow({ label, value }: { label: string; value: number | null }) {
  const s = value ?? 0;
  const [r, g, b] = scoreToColor(s);
  return (
    <div className="mb-2.5">
      <div className="flex justify-between text-xs mb-1">
        <span className="text-[var(--text-dim)]">{label}</span>
        <span className="font-mono font-semibold" style={{ color: `rgb(${r},${g},${b})` }}>
          {value !== null ? s.toFixed(1) : '—'}
        </span>
      </div>
      <div className="h-1.5 rounded-full" style={{ background: 'var(--border-2)' }}>
        <div
          className="h-full rounded-full transition-all duration-700"
          style={{ width: `${s}%`, background: `rgb(${r},${g},${b})` }}
        />
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────
export default function CityDetailPanel({ detail, onClose, onTradeAreaLoaded }: Props) {
  const [loadingInsights, setLoadingInsights] = useState(false);
  const [insights, setInsights] = useState<string | null>(null);
  const [insightsOpen, setInsightsOpen] = useState(false);
  const [loadingTradeArea, setLoadingTradeArea] = useState(false);
  const [tradeAreaItems, setTradeAreaItems] = useState<TradeAreaItem[] | null>(null);
  const [tradeAreaTotal, setTradeAreaTotal] = useState<number | null>(null);
  const [tradeAreaNarrative, setTradeAreaNarrative] = useState<string | null>(null);
  const [loadingTradeAreaInsights, setLoadingTradeAreaInsights] = useState(false);
  const [tradeAreaInsightsOpen, setTradeAreaInsightsOpen] = useState(false);

  const tierC = tierColor(detail.tier);
  const d = detail.demograficos;
  const e = detail.estabelecimentos;
  const ec = detail.economicos;
  const s = detail.score;
  const hasCoords = detail.latitude !== null && detail.longitude !== null;

  const handleInsights = async () => {
    if (insights) { setInsightsOpen(o => !o); return; }
    setLoadingInsights(true);
    try {
      const res = await api.postInsights(detail.codigo_ibge);
      setInsights(res.narrative);
      setInsightsOpen(true);
    } catch {
      setInsights('Erro ao carregar insights. Verifique se ANTHROPIC_API_KEY está configurada.');
      setInsightsOpen(true);
    } finally {
      setLoadingInsights(false);
    }
  };

  const handleSimulate = async () => {
    if (!hasCoords) return;
    setLoadingTradeArea(true);
    setTradeAreaItems(null);
    setTradeAreaTotal(null);
    setTradeAreaNarrative(null);
    setTradeAreaInsightsOpen(false);
    try {
      const res = await api.getTradeArea(detail.latitude!, detail.longitude!, 200);
      setTradeAreaItems(res.results);
      setTradeAreaTotal(res.total_estimated_customers ?? null);
      onTradeAreaLoaded([detail.longitude!, detail.latitude!], res.results);
    } catch {
      setTradeAreaItems([]);
    } finally {
      setLoadingTradeArea(false);
    }
  };

  const handleTradeAreaInsights = async () => {
    if (tradeAreaNarrative) { setTradeAreaInsightsOpen(o => !o); return; }
    if (!tradeAreaItems || tradeAreaItems.length === 0) return;
    setLoadingTradeAreaInsights(true);
    try {
      const payload: TradeAreaInsightsRequest = {
        codigo_ibge: detail.codigo_ibge,
        center_lat: detail.latitude!,
        center_lon: detail.longitude!,
        radius_km: 200,
        total_estimated_customers: tradeAreaTotal,
        items: tradeAreaItems,
      };
      const res = await api.postTradeAreaInsights(payload);
      setTradeAreaNarrative(res.narrative);
      setTradeAreaInsightsOpen(true);
    } catch {
      setTradeAreaNarrative('Erro ao gerar análise estratégica. Verifique se ANTHROPIC_API_KEY está configurada.');
      setTradeAreaInsightsOpen(true);
    } finally {
      setLoadingTradeAreaInsights(false);
    }
  };

  return (
    <div
      className="slide-in-right absolute right-0 top-0 h-full z-30 flex flex-col overflow-hidden"
      style={{
        width: 440,
        background: 'var(--navy-2)',
        borderLeft: '1px solid var(--border)',
      }}
    >
      {/* Header */}
      <div
        className="flex items-start justify-between p-4 border-b flex-shrink-0"
        style={{ borderColor: 'var(--border)' }}
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="font-bold text-base truncate">{detail.nome}</h2>
            {detail.tier && (
              <span
                className="text-xs font-bold px-2 py-0.5 rounded-full"
                style={{ background: `${tierC}22`, color: tierC, border: `1px solid ${tierC}55` }}
              >
                Tier {detail.tier}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1 text-xs text-[var(--text-dim)] mt-0.5">
            <MapPin size={11} />
            {detail.uf}
            {detail.regiao && ` · ${detail.regiao}`}
            {detail.ranking_nacional && (
              <span className="ml-1 text-[var(--primary)]">#{detail.ranking_nacional} nacional</span>
            )}
          </div>
        </div>
        <button
          onClick={onClose}
          className="flex-shrink-0 ml-2 p-1 rounded-lg hover:bg-[var(--glass)] transition-colors"
        >
          <X size={16} className="text-[var(--muted)]" />
        </button>
      </div>

      {/* Scrollable body */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4">

        {/* Stat cards 2×2 / 2x3 */}
        <div className="grid grid-cols-2 gap-2">
          <StatCard
            icon={Users}
            label="Pop. Alvo (30–64)"
            value={fmt(d?.populacao_alvo)}
            sub={d?.pct_populacao_alvo !== null ? `${pct(d?.pct_populacao_alvo)} da população` : undefined}
          />
          <StatCard
            icon={Building2}
            label="Urbanização"
            value={pct(d?.taxa_urbanizacao)}
            sub={d?.populacao_total !== null ? `${fmt(d?.populacao_total)} pop. total` : undefined}
          />
          <StatCard
            icon={Users}
            label="Envelhecimento"
            value={fmt(d?.indice_envelhecimento, '', 1)}
            sub="Idosos p/ 100 jovens"
          />
          <StatCard
            icon={DollarSign}
            label="PIB per Capita"
            value={fmt(ec?.pib_per_capita, 'R$ ')}
            sub={ec?.idh !== null ? `IDH ${ec?.idh?.toFixed(3)}` : undefined}
          />
          <StatCard
            icon={Building2}
            label="Farmácias"
            value={fmt(e?.farmacias)}
            sub={e?.farmacias_por_10k !== null ? `${e?.farmacias_por_10k?.toFixed(1)}/10k hab.` : undefined}
          />
          <StatCard
            icon={Shield}
            label="Cob. Planos"
            value={pct(ec?.cobertura_planos_pct)}
            sub={ec?.beneficiarios_planos !== null ? `${fmt(ec?.beneficiarios_planos)} beneficiários` : undefined}
          />
        </div>

        {/* Health infrastructure row */}
        {e && (
          <div className="glass p-3">
            <div className="flex items-center gap-1.5 text-[var(--muted)] text-[10px] mb-2">
              <Building2 size={11} /> Infraestrutura de Saúde
            </div>
            <div className="grid grid-cols-3 gap-2 text-center">
              {[
                { label: 'Clínicas', value: e.clinicas },
                { label: 'Laboratórios', value: e.laboratorios },
                { label: 'Hospitais', value: e.hospitais },
                { label: 'Consultórios Méd.', value: e.consultorios_medicos },
                { label: 'Consultórios Odonto', value: e.consultorios_odonto },
                { label: 'UBS/UPA', value: e.ubs_upa },
              ].map(({ label, value }) => (
                <div key={label}>
                  <div className="text-base font-bold">{fmt(value)}</div>
                  <div className="text-[9px] text-[var(--text-dim)]">{label}</div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Score breakdown */}
        <div className="glass p-3">
          <div className="flex items-center gap-1.5 text-[var(--muted)] text-[10px] mb-3">
            <BarChart3 size={11} /> Score Breakdown
          </div>
          <ScoreRow label="Demográfico"        value={s?.score_demografico ?? null} />
          <ScoreRow label="Infra. Saúde"       value={s?.score_infraestrutura_saude ?? null} />
          <ScoreRow label="Econômico"          value={s?.score_economico ?? null} />
          <ScoreRow label="Logístico"          value={s?.score_logistico ?? null} />
          <ScoreRow label="Competitividade"    value={s?.score_competitividade ?? null} />
          <div className="mt-3 pt-3 border-t" style={{ borderColor: 'var(--border)' }}>
            <ScoreRow label="Score Total" value={detail.score_total ?? null} />
          </div>
        </div>

        {/* Trade Area results */}
        {tradeAreaItems && tradeAreaItems.length > 0 && (
          <div className="glass p-3">
            <div className="flex items-center gap-1.5 text-amber-400 text-[10px] mb-2">
              <Zap size={11} /> Área de Influência — Top 5 cidades atraídas
            </div>
            <div className="space-y-1.5">
              {tradeAreaItems.slice(0, 5).map(item => (
                <div key={item.codigo_ibge} className="flex items-center gap-2 text-xs">
                  <div className="flex-1 truncate text-[var(--text-dim)]">
                    {item.nome} <span className="text-[10px]">({item.uf})</span>
                  </div>
                  <div className="text-right">
                    <div className="font-mono text-amber-400 text-[10px]">
                      {(item.probability * 100).toFixed(1)}%
                    </div>
                    <div className="text-[9px] text-[var(--muted)]">{item.distance_km.toFixed(0)} km</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Trade Area Strategic Analysis narrative */}
        {tradeAreaNarrative && tradeAreaInsightsOpen && (
          <div className="glass p-3">
            <div className="flex items-center gap-1.5 text-amber-400 text-[10px] mb-2">
              <Brain size={11} /> Análise Estratégica da Área — Claude
            </div>
            <div className="text-xs text-[var(--text-dim)] leading-relaxed max-h-80 overflow-y-auto">
              <MarkdownBlock text={tradeAreaNarrative} />
            </div>
          </div>
        )}

        {/* IA Insights */}
        {insights && insightsOpen && (
          <div className="glass p-3">
            <div className="flex items-center gap-1.5 text-[var(--primary)] text-[10px] mb-2">
              <Brain size={11} /> Análise IA — Claude claude-sonnet-4-6
            </div>
            <div className="text-xs text-[var(--text-dim)] leading-relaxed max-h-80 overflow-y-auto">
              <MarkdownBlock text={insights} />
            </div>
          </div>
        )}
      </div>

      {/* Footer actions */}
      <div className="p-4 border-t space-y-2 flex-shrink-0" style={{ borderColor: 'var(--border)' }}>
        {/* Simulate Store Opening */}
        <button
          onClick={handleSimulate}
          disabled={!hasCoords || loadingTradeArea}
          title={!hasCoords ? 'Coordenadas não disponíveis para este município' : undefined}
          className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-sm font-semibold transition-all"
          style={{
            background: hasCoords ? 'rgba(245, 158, 11, 0.15)' : 'var(--glass)',
            color: hasCoords ? '#f59e0b' : 'var(--muted)',
            border: `1px solid ${hasCoords ? 'rgba(245,158,11,0.4)' : 'var(--border)'}`,
            cursor: hasCoords ? 'pointer' : 'not-allowed',
          }}
        >
          {loadingTradeArea
            ? <><Loader2 size={14} className="animate-spin" /> Calculando...</>
            : <><Zap size={14} /> Simular Abertura de Loja</>
          }
        </button>

        {/* Trade Area Strategic Analysis button — only visible after simulation */}
        {tradeAreaItems && tradeAreaItems.length > 0 && (
          <button
            onClick={handleTradeAreaInsights}
            disabled={loadingTradeAreaInsights}
            className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-sm font-semibold transition-all"
            style={{
              background: 'rgba(245, 158, 11, 0.1)',
              color: '#f59e0b',
              border: '1px solid rgba(245,158,11,0.3)',
              cursor: 'pointer',
            }}
          >
            {loadingTradeAreaInsights
              ? <><Loader2 size={14} className="animate-spin" /> Gerando análise...</>
              : tradeAreaNarrative
                ? <>{tradeAreaInsightsOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />} {tradeAreaInsightsOpen ? 'Ocultar' : 'Exibir'} Análise Estratégica</>
                : <><Zap size={14} /> Gerar Análise Estratégica da Área</>
            }
          </button>
        )}

        {/* IA Insights */}
        <button
          onClick={handleInsights}
          disabled={loadingInsights}
          className="w-full flex items-center justify-center gap-2 py-2.5 rounded-xl text-sm font-semibold transition-all"
          style={{
            background: 'var(--primary-dim)',
            color: 'var(--primary)',
            border: '1px solid rgba(56,189,248,0.3)',
            cursor: 'pointer',
          }}
        >
          {loadingInsights
            ? <><Loader2 size={14} className="animate-spin" /> Gerando análise...</>
            : insights
              ? <>{insightsOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />} {insightsOpen ? 'Ocultar' : 'Exibir'} Análise IA</>
              : <><Brain size={14} /> Gerar Análise IA</>
          }
        </button>
      </div>
    </div>
  );
}
