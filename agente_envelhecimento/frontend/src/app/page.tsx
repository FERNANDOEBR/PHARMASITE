'use client';

import { useState, useEffect, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { Activity, Database } from 'lucide-react';

import RankingSidebar from '@/components/RankingSidebar';
import CityDetailPanel from '@/components/CityDetailPanel';
import { api } from '@/lib/api';
import type { Municipio, MunicipioDetail, TradeAreaItem } from '@/lib/types';

// Map must be client-only (no SSR — WebGL requires browser)
const PharmaSiteMap = dynamic(() => import('@/components/Map'), { ssr: false });

export default function Dashboard() {
  const [municipalities, setMunicipalities] = useState<Municipio[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [uf, setUf] = useState('');
  const [cityDetail, setCityDetail] = useState<MunicipioDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [tradeArea, setTradeArea] = useState<{ center: [number, number]; items: TradeAreaItem[] } | null>(null);

  // Fetch municipalities for the map on mount
  useEffect(() => {
    api.getMunicipios({ limit: 6000 })
      .then(res => setMunicipalities(res.results))
      .catch(console.error);
  }, []);

  // Fetch city detail whenever selectedId changes
  useEffect(() => {
    if (!selectedId) { setCityDetail(null); setTradeArea(null); return; }
    setLoadingDetail(true);
    setTradeArea(null);
    api.getMunicipio(selectedId)
      .then(setCityDetail)
      .catch(console.error)
      .finally(() => setLoadingDetail(false));
  }, [selectedId]);

  const handleCitySelect = useCallback((id: string) => {
    setSelectedId(prev => prev === id ? null : id);
  }, []);

  const handleClose = useCallback(() => {
    setSelectedId(null);
    setCityDetail(null);
    setTradeArea(null);
  }, []);

  const handleTradeAreaLoaded = useCallback(
    (center: [number, number], items: TradeAreaItem[]) => {
      setTradeArea({ center, items });
    },
    [],
  );

  const coordCount = municipalities.filter(m => m.latitude !== null).length;

  return (
    <div className="flex h-screen w-screen overflow-hidden" style={{ background: 'var(--navy)' }}>

      {/* ── Ranking Sidebar ─────────────────────────────────────────────── */}
      <RankingSidebar selectedId={selectedId} onCitySelect={handleCitySelect} uf={uf} onUfChange={setUf} />

      {/* ── Main content: Map + optional detail panel ───────────────────── */}
      <div className="flex-1 relative overflow-hidden">

        {/* Top bar */}
        <div
          className="absolute top-0 left-0 right-0 z-20 flex items-center justify-between px-5 py-3"
          style={{
            background: 'rgba(5,11,30,0.85)',
            backdropFilter: 'blur(12px)',
            borderBottom: '1px solid var(--border)',
          }}
        >
          <div className="flex items-center gap-2.5">
            <div
              className="w-7 h-7 rounded-lg flex items-center justify-center"
              style={{ background: 'var(--primary-dim)', border: '1px solid rgba(56,189,248,0.3)' }}
            >
              <Activity size={14} className="text-[var(--primary)]" />
            </div>
            <div>
              <span className="font-bold text-sm tracking-tight">PharmaSite Intelligence</span>
              <span className="ml-2 text-[10px] text-[var(--muted)]">
                Inteligência de Mercado Farmacêutico
              </span>
            </div>
          </div>

          <div className="flex items-center gap-3 text-[11px] text-[var(--muted)]">
            <div className="flex items-center gap-1.5">
              <Database size={11} />
              {municipalities.length.toLocaleString()} municípios
            </div>
            {coordCount > 0 && (
              <div className="glass px-2 py-1 text-[10px]">
                {coordCount} com coordenadas
              </div>
            )}
            <div
              className="w-2 h-2 rounded-full animate-pulse"
              style={{ background: '#22c55e' }}
              title="API conectada"
            />
          </div>
        </div>

        {/* Map canvas (fills remaining height below top bar) */}
        <div className="absolute inset-0 pt-11">
          <PharmaSiteMap
            municipalities={municipalities}
            selectedId={selectedId}
            uf={uf}
            tradeAreaData={tradeArea}
            onCityClick={handleCitySelect}
          />
        </div>

        {/* Loading overlay while fetching city detail */}
        {loadingDetail && (
          <div
            className="absolute right-0 top-11 bottom-0 z-30 flex items-center justify-center"
            style={{
              width: 440,
              background: 'rgba(5,11,30,0.7)',
              backdropFilter: 'blur(8px)',
            }}
          >
            <div className="glass px-6 py-4 flex flex-col items-center gap-2">
              <div className="w-6 h-6 border-2 border-[var(--primary)] border-t-transparent rounded-full animate-spin" />
              <div className="text-xs text-[var(--text-dim)]">Carregando dados...</div>
            </div>
          </div>
        )}

        {/* City detail panel */}
        {cityDetail && !loadingDetail && (
          <CityDetailPanel
            detail={cityDetail}
            onClose={handleClose}
            onTradeAreaLoaded={handleTradeAreaLoaded}
          />
        )}
      </div>
    </div>
  );
}
