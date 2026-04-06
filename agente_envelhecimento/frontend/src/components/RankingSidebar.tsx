'use client';

import { useState, useEffect } from 'react';
import { TrendingUp, Filter } from 'lucide-react';
import { api } from '@/lib/api';
import type { RankingItem } from '@/lib/types';
import { tierColor, scoreToColor } from '@/lib/colors';

const UFS = [
  'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MG', 'MS', 'MT',
  'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO',
];

interface Props {
  selectedId: string | null;
  onCitySelect: (codigoIbge: string) => void;
  uf: string;
  onUfChange: (uf: string) => void;
}

function ScoreBar({ score }: { score: number | null }) {
  const s = score ?? 0;
  const [r, g, b] = scoreToColor(s);
  return (
    <div className="w-full h-1 rounded-full mt-1" style={{ background: 'var(--border-2)' }}>
      <div
        className="h-full rounded-full transition-all duration-500"
        style={{ width: `${s}%`, background: `rgb(${r},${g},${b})` }}
      />
    </div>
  );
}

function SkeletonCard() {
  return (
    <div className="glass p-3 flex gap-3">
      <div className="skeleton w-8 h-8 rounded-lg flex-shrink-0" />
      <div className="flex-1 space-y-1.5">
        <div className="skeleton h-3 w-3/4" />
        <div className="skeleton h-2 w-1/2" />
        <div className="skeleton h-1 w-full" />
      </div>
    </div>
  );
}

export default function RankingSidebar({ selectedId, onCitySelect, uf, onUfChange }: Props) {
  const [ranking, setRanking] = useState<RankingItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState(false);
  const [tier, setTier] = useState('');

  useEffect(() => {
    setLoading(true);
    setFetchError(false);
    api.getRanking({ limit: 50, ...(uf ? { uf } : {}), ...(tier ? { tier } : {}) })
      .then(res => {
        const data = res.results ?? [];
        // Deduplicate by codigo_ibge to prevent key errors
        const unique = data.filter((item, i, self) =>
          i === self.findIndex(t => t.codigo_ibge === item.codigo_ibge)
        );
        setRanking(unique);
      })
      .catch(() => { setFetchError(true); setRanking([]); })
      .finally(() => setLoading(false));
  }, [uf, tier]);

  return (
    <aside
      className="relative z-20 flex flex-col h-full"
      style={{ width: 340, background: 'var(--navy-2)', borderRight: '1px solid var(--border)' }}
    >
      {/* Header */}
      <div className="p-4 border-b" style={{ borderColor: 'var(--border)' }}>
        <div className="flex items-center gap-2 mb-3">
          <TrendingUp size={18} className="text-[var(--primary)]" />
          <h2 className="font-semibold text-sm tracking-wide">Top Municípios</h2>
        </div>

        <div className="flex gap-2">
          {/* UF filter */}
          <div className="relative flex-1">
            <Filter size={13} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-[var(--muted)]" />
            <select
              value={uf}
              onChange={e => onUfChange(e.target.value)}
              className="w-full pl-7 pr-1 py-1.5 text-xs rounded-lg appearance-none cursor-pointer"
              style={{
                background: 'var(--glass)',
                border: '1px solid var(--border)',
                color: 'var(--text)',
              }}
            >
              <option value="">Brasil</option>
              {UFS.map(u => <option key={u} value={u}>{u}</option>)}
            </select>
          </div>

          {/* Tier filter */}
          <div className="relative flex-1">
            <select
              value={tier}
              onChange={e => setTier(e.target.value)}
              className="w-full px-3 py-1.5 text-xs rounded-lg appearance-none cursor-pointer"
              style={{
                background: 'var(--glass)',
                border: '1px solid var(--border)',
                color: 'var(--text)',
              }}
            >
              <option value="">Qualquer Tier</option>
              {['A', 'B', 'C', 'D'].map(t => <option key={t} value={t}>Tier {t}</option>)}
            </select>
          </div>
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto p-3 space-y-2">
        {loading
          ? Array.from({ length: 8 }).map((_, i) => <SkeletonCard key={i} />)
          : ranking.map((item, idx) => {
            const isSelected = item.codigo_ibge === selectedId;
            const color = tierColor(item.tier);
            return (
              <button
                key={`${item.codigo_ibge}-${idx}`}
                onClick={() => onCitySelect(item.codigo_ibge)}
                className="w-full text-left glass p-3 flex gap-3 transition-all duration-200 cursor-pointer group"
                style={{
                  borderColor: isSelected ? 'var(--primary)' : undefined,
                  background: isSelected ? 'var(--primary-dim)' : undefined,
                }}
              >
                {/* Rank badge */}
                <div
                  className="flex-shrink-0 w-8 h-8 rounded-lg flex items-center justify-center text-xs font-bold"
                  style={{ background: 'var(--navy-3)', color: 'var(--text-dim)' }}
                >
                  {item.ranking_nacional ?? idx + 1}
                </div>

                {/* Info */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between gap-1">
                    <span className="text-xs font-medium truncate">{item.nome}</span>
                    {item.tier && (
                      <span
                        className="flex-shrink-0 text-[10px] font-bold px-1.5 py-0.5 rounded"
                        style={{ background: `${color}22`, color }}
                      >
                        {item.tier}
                      </span>
                    )}
                  </div>
                  <div className="text-[10px] text-[var(--text-dim)] mt-0.5">
                    {item.uf}
                    {item.score_total !== null && (
                      <span className="ml-2 text-[var(--primary)] font-mono">
                        {item.score_total.toFixed(1)}
                      </span>
                    )}
                  </div>
                  <ScoreBar score={item.score_total} />
                </div>
              </button>
            );
          })}

        {!loading && ranking.length === 0 && (
          <div className="text-center text-xs text-[var(--muted)] py-8">
            {fetchError
              ? 'Erro ao carregar ranking. Verifique a conexão com a API.'
              : 'Nenhum município encontrado'}
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="px-4 py-2 border-t text-[10px] text-[var(--muted)]" style={{ borderColor: 'var(--border)' }}>
        {!loading && `${ranking.length} municípios exibidos`}
      </div>
    </aside>
  );
}
