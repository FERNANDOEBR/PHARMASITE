'use client';

import { useState } from 'react';
import {
  X, Users, DollarSign, Building2, Shield, BarChart3,
  Brain, MapPin, Loader2, ChevronDown, ChevronUp, Zap,
} from 'lucide-react';
import { api } from '@/lib/api';
import type { MunicipioDetail, TradeAreaItem, TradeAreaInsightsRequest, MicrobairroItem } from '@/lib/types';
import { tierColor, scoreToColor } from '@/lib/colors';

interface Props {
  detail: MunicipioDetail;
  microbairros: MicrobairroItem[] | null;
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
export default function CityDetailPanel({ detail, microbairros, onClose, onTradeAreaLoaded }: Props) {
  const [loadingInsights, setLoadingInsights] = useState(false);
  const [insights, setInsights] = useState<string | null>(null);
  const [insightsOpen, setInsightsOpen] = useState(false);
  const [loadingTradeArea, setLoadingTradeArea] = useState(false);
  const [tradeAreaItems, setTradeAreaItems] = useState<TradeAreaItem[] | null>(null);
  const [tradeAreaTotal, setTradeAreaTotal] = useState<number | null>(null);
  const [tradeAreaNarrative, setTradeAreaNarrative] = useState<string | null>(null);
  const [loadingTradeAreaInsights, setLoadingTradeAreaInsights] = useState(false);
  const [tradeAreaInsightsOpen, setTradeAreaInsightsOpen] = useState(false);
  
  const [loadingL2Insights, setLoadingL2Insights] = useState(false);
  const [l2Narrative, setL2Narrative] = useState<string | null>(null);
  const [l2InsightsOpen, setL2InsightsOpen] = useState(false);
  
  const [loadingScouter, setLoadingScouter] = useState(false);
  const [scouterResult, setScouterResult] = useState<any>(null);

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
        center_lat: tradeAreaItems[0].latitude!,
        center_lon: tradeAreaItems[0].longitude!,
        radius_km: 30, // Default query was 30km
        total_estimated_customers: tradeAreaTotal,
        items: tradeAreaItems.slice(0, 8), // send top 8 to save tokens
        indice_envelhecimento: detail.demograficos?.indice_envelhecimento ?? null,
        pop_0_4: detail.demograficos?.pop_0_4 ?? null,
        pop_5_14: detail.demograficos?.pop_5_14 ?? null,
        pop_15_29: detail.demograficos?.pop_15_29 ?? null,
        pop_30_59: detail.demograficos?.pop_30_59 ?? null,
        pop_60_mais: detail.demograficos?.pop_60_mais ?? null,
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

  const handleL2Insights = async () => {
    if (l2Narrative) { setL2InsightsOpen(o => !o); return; }
    if (!microbairros || microbairros.length === 0) return;
    setLoadingL2Insights(true);
    try {
      const payload = {
        city: detail.nome,
        items: microbairros,
      };
      const res = await api.postMicrobairrosInsights(detail.codigo_ibge, payload);
      setL2Narrative(res.narrative);
      setL2InsightsOpen(true);
    } catch {
      setL2Narrative('Erro ao gerar AI Pitch L2.');
      setL2InsightsOpen(true);
    } finally {
      setLoadingL2Insights(false);
    }
  };

  const handleRunScouter = async () => {
    setLoadingScouter(true);
    try {
      // Direct call to /scout fallback in api.py
      const url = `${process.env.NEXT_PUBLIC_API_URL ?? 'http://127.0.0.1:8000'}/scout?cidade=${detail.nome}, ${detail.uf}&bairro=Geral`;
      const res = await fetch(url);
      if(!res.ok) throw new Error("Scout Failed");
      const data = await res.json();
      setScouterResult(data);
    } catch (e) {
      setScouterResult({ score: 0, justificativa: "Falha ao executar o agente scouter." });
    } finally {
      setLoadingScouter(false);
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
            label="Pop. Idosa (65+)"
            value={d?.elderly_pct !== null && d?.elderly_pct !== undefined ? `${d.elderly_pct.toFixed(1)}%` : '—'}
            sub={d?.indice_envelhecimento ? `Índ. Env: ${d.indice_envelhecimento.toFixed(1)}` : `do total da população`}
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

        {/* L2 Microbairros Analysis */}
        {microbairros && microbairros.length > 0 ? (
          <div className="glass p-3">
            <div className="flex items-center gap-1.5 text-rose-400 text-[10px] mb-2">
              <MapPin size={11} /> Gaps L2 Microbairros — Top 5 Oportunidades
            </div>
            <div className="space-y-2">
              {microbairros.slice(0, 5).map(m => (
                <div key={m.Microbairro} className="flex items-center gap-2 text-xs">
                  <div className="flex-1 truncate font-semibold text-white">
                    {m.Microbairro} <span className="text-[10px] text-[var(--text-dim)] font-normal ml-1">R${m.City_Income_Proxy}</span>
                  </div>
                  <div className="text-right">
                    <div className="font-mono text-rose-400 text-xs">
                      {m.Opportunity_Score.toFixed(1)}
                    </div>
                    <div className="text-[9px] text-[var(--muted)]">{m.Mapped_Pharmacies} PDVs ({m.Big_Chains_Mapped} Redes)</div>
                  </div>
                </div>
              ))}
            </div>
            
            <button
               onClick={handleL2Insights}
               disabled={loadingL2Insights}
               className="w-full mt-3 flex items-center justify-center gap-2 py-2.0 rounded-lg text-xs font-semibold transition-all border"
               style={{
                 background: 'rgba(244, 63, 94, 0.15)',
                 color: '#fb7185',
                 borderColor: 'rgba(244, 63, 94, 0.4)',
               }}
            >
              {loadingL2Insights ? <><Loader2 size={12} className="animate-spin" /> Gerando Pitch...</> : <><Brain size={12} /> Gerar Pitch de Negócios L2 (IA)</>}
            </button>

            {/* AI Pitch Narrative View */}
            {l2Narrative && l2InsightsOpen && (
              <div className="mt-2 text-xs text-[var(--text-dim)] leading-relaxed max-h-60 overflow-y-auto border-t border-[rgba(244,63,94,0.3)] pt-2">
                <MarkdownBlock text={l2Narrative} />
              </div>
            )}
          </div>
        ) : microbairros && microbairros.length === 0 ? (
          <div className="glass p-3 border-dashed" style={{ borderColor: 'var(--border)' }}>
            <div className="flex items-center gap-1.5 text-[var(--muted)] text-[10px] mb-2">
              <MapPin size={11} /> OpenStreetMap (L2) Edge Case
            </div>
            <div className="text-xs text-[var(--text-dim)] mb-2">
              Não foram encontrados polígonos mapeados para os microbairros de <strong>{detail.nome}</strong> no OSM. Este é um blindspot clássico de mapeamento ativo.
            </div>
            {scouterResult ? (
               <div className="mt-2 p-3 rounded text-xs glass bg-opacity-30 border border-[rgba(56,189,248,0.3)]">
                 <div className="flex items-center justify-between mb-2">
                   <div className="text-[var(--primary)] font-bold">Resumo AI Scouter</div>
                   <div className="glass px-2 py-0.5 rounded text-white font-mono">{scouterResult.growth_score}/100</div>
                 </div>
                 
                 <div className="text-[var(--text-dim)] mb-2 mt-1 italic">"{scouterResult.verdict}"</div>
                 <MarkdownBlock text={scouterResult.analysis_markdown || scouterResult.justificativa} />
                 
                 {scouterResult.sources && scouterResult.sources.length > 0 && (
                   <div className="mt-3 pt-2 border-t border-[rgba(255,255,255,0.1)]">
                     <div className="text-[10px] text-gray-400 font-semibold mb-1 uppercase tracking-wider">Fontes (DDGS)</div>
                     <ul className="list-disc list-inside space-y-1">
                       {scouterResult.sources.map((s: any, i: number) => (
                         <li key={i} className="text-[10px] truncate max-w-full">
                           <a href={s.url} target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:underline">
                             {s.title}
                           </a>
                         </li>
                       ))}
                     </ul>
                   </div>
                 )}
               </div>
            ) : (
              <button
                 onClick={handleRunScouter}
                 disabled={loadingScouter}
                 className="w-full py-2 rounded-lg text-xs font-semibold transition-all flex items-center justify-center gap-2"
                 style={{ background: 'var(--navy)', color: 'var(--primary)', border: '1px solid rgba(56,189,248,0.4)' }}
              >
                {loadingScouter ? <><Loader2 size={12} className="animate-spin" /> Agente Coletando Informações...</> : <><Brain size={12}/> Run Agent Scouter (Web Research)</>}
              </button>
            )}
          </div>
        ) : null}

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
