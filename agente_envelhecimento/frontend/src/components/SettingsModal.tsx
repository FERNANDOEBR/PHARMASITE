import React, { useState, useEffect } from 'react';
import { Settings, Save, X, Database, Sliders, Calculator, ActivitySquare } from 'lucide-react';
import { api } from '@/lib/api';
import type { ScenarioConfig } from '@/lib/types';

interface SettingsModalProps {
  onClose: () => void;
  onSaveConfig: () => void;
}

export default function SettingsModal({ onClose, onSaveConfig }: SettingsModalProps) {
  const [config, setConfig] = useState<ScenarioConfig>({
    sales_data_path: "C:\\Users\\ferna\\Downloads\\relatorio de vendas BRB.xlsx",
    max_viable_km: 200,
    min_population: 0,
    use_custom_weights: false,
    weights: {
      demo: 0.19,
      logistica: 0.40,
      economia: 0.22,
      saude: 0.10,
      competitividade: 0.08
    }
  });

  const [loading, setLoading] = useState(false);
  const [fetching, setFetching] = useState(true);

  useEffect(() => {
    api.getActiveScenario()
      .then((res) => {
        if (res.config) {
          setConfig(res.config);
        } else if (res.weights) {
          // Backward compatibility if backend returned old format
          setConfig(c => ({ ...c, weights: res.weights }));
        }
      })
      .catch((err) => console.error("Failed to load scenario settings", err))
      .finally(() => setFetching(false));
  }, []);

  const handleConfigChange = <K extends keyof ScenarioConfig>(key: K, val: ScenarioConfig[K]) => {
    setConfig(prev => ({ ...prev, [key]: val }));
  };

  const handleWeightChange = (key: keyof ScenarioConfig['weights'], val: number) => {
    setConfig(prev => ({
      ...prev,
      weights: { ...prev.weights, [key]: val }
    }));
  };

  const handleSave = async () => {
    setLoading(true);
    try {
      await api.saveScenario(config);
      onSaveConfig();
      onClose();
    } catch (err) {
      console.error(err);
      alert("Erro ao salvar cenário e recalcular scores.");
    } finally {
      setLoading(false);
    }
  };

  if (fetching) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
        <div className="w-6 h-6 border-2 border-[var(--primary)] border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm pointer-events-auto">
      {/* Click-away backdrop */}
      <div className="absolute inset-0" onClick={onClose} />

      <div className="relative glass w-[500px] border border-[var(--border)] rounded-xl overflow-hidden animate-in fade-in zoom-in-95 duration-200 shadow-2xl flex flex-col max-h-[90vh]">
        
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-[var(--border)] bg-[rgba(5,11,30,0.9)] shrink-0">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-[var(--primary-dim)] flex items-center justify-center border border-[rgba(56,189,248,0.3)] shadow-[0_0_15px_rgba(56,189,248,0.2)]">
               <ActivitySquare size={20} className="text-[var(--primary)]" />
            </div>
            <div>
              <h2 className="text-base font-bold text-white tracking-tight">Painel de Controle</h2>
              <p className="text-[11px] text-[var(--muted)]">Configure a inteligência do motor PharmaSite</p>
            </div>
          </div>
          <button onClick={onClose} className="p-2 -mr-2 text-[var(--muted)] hover:text-white transition-colors">
            <X size={20} />
          </button>
        </div>

        {/* Scrollable Body */}
        <div className="p-5 flex flex-col gap-6 bg-[rgba(5,11,30,0.5)] overflow-y-auto custom-scrollbar">

          {/* Section 1: Data Source */}
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-white font-semibold border-b border-white/10 pb-2">
              <Database size={16} className="text-[var(--primary)]" />
              <h4>Calibração de Base de Vendas</h4>
            </div>
            <div className="space-y-1">
              <label className="text-xs text-[var(--muted)] block">Caminho do Arquivo (Excel)</label>
              <input 
                type="text" 
                value={config.sales_data_path}
                onChange={(e) => handleConfigChange('sales_data_path', e.target.value)}
                placeholder="Ex: C:\caminho\para\vendas.xlsx"
                className="w-full bg-[rgba(0,0,0,0.3)] border border-[var(--border)] rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-[var(--primary)] transition-colors"
                spellCheck={false}
              />
              <p className="text-[10px] text-zinc-500 pt-1">
                Utilizado para o motor ajustar algoritmicamente a importância de cada pilar usando regressão multivariada (NNLS).
              </p>
            </div>
          </div>

          {/* Section 2: Viability Rules */}
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-white font-semibold border-b border-white/10 pb-2">
              <Sliders size={16} className="text-orange-400" />
              <h4>Regras de Viabilidade</h4>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-1">
                <label className="text-xs text-[var(--muted)] block">Raio Logístico Máximo (Km)</label>
                <input 
                  type="number" 
                  value={config.max_viable_km}
                  onChange={(e) => handleConfigChange('max_viable_km', parseFloat(e.target.value) || 0)}
                  className="w-full bg-[rgba(0,0,0,0.3)] border border-[var(--border)] rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-orange-500 transition-colors"
                />
              </div>
              <div className="space-y-1">
                <label className="text-xs text-[var(--muted)] block">População Mínima</label>
                <input 
                  type="number" 
                  value={config.min_population}
                  onChange={(e) => handleConfigChange('min_population', parseInt(e.target.value) || 0)}
                  className="w-full bg-[rgba(0,0,0,0.3)] border border-[var(--border)] rounded px-3 py-2 text-sm text-white focus:outline-none focus:border-orange-500 transition-colors"
                />
              </div>
            </div>
          </div>

          {/* Section 3: Fine-Tuning */}
          <div className="space-y-3">
            <div className="flex items-center justify-between border-b border-white/10 pb-2">
              <div className="flex items-center gap-2 text-white font-semibold">
                <Calculator size={16} className="text-purple-400" />
                <h4>Ajuste Fino de Algoritmo</h4>
              </div>
              {/* Toggle Switch */}
              <button 
                onClick={() => handleConfigChange('use_custom_weights', !config.use_custom_weights)}
                className={`flex items-center w-8 h-4 rounded-full transition-colors p-0.5 ${config.use_custom_weights ? 'bg-purple-500' : 'bg-gray-600'}`}
              >
                <div className={`w-3 h-3 bg-white rounded-full transition-transform ${config.use_custom_weights ? 'translate-x-4' : 'translate-x-0'}`} />
              </button>
            </div>
            
            {!config.use_custom_weights ? (
              <div className="bg-[rgba(168,85,247,0.1)] border border-[rgba(168,85,247,0.2)] rounded p-3 text-xs text-[#d8b4fe] leading-relaxed">
                <strong>Otimização Automática Ativa.</strong> Sempre que o motor recalcular, ele correrá os dados de vendas fornecidos acima para determinar os pesos perfeitos (Minimos Quadrados).
              </div>
            ) : (
              <div className="space-y-4 animate-in slide-in-from-top-2 duration-300">
                <div className="text-xs text-zinc-400 mb-2">Deslizadores habilitados para configuração manual (a priori) forçada:</div>
                {Object.entries({
                  'Logística (Proximidade)': 'logistica',
                  'Economia (Renda)': 'economia',
                  'Demografia (População)': 'demo',
                  'Infraestrutura de Saúde': 'saude',
                  'Competitividade (Concorrência)': 'competitividade'
                }).map(([label, key]) => {
                  const val = config.weights[key as keyof ScenarioConfig['weights']] || 0;
                  const pct = Math.round(val * 100);
                  return (
                    <div key={key} className="flex flex-col gap-1.5">
                      <div className="flex justify-between text-[11px] font-semibold">
                        <span className="text-[var(--text-dim)]">{label}</span>
                        <span className="text-purple-400">{pct}%</span>
                      </div>
                      <input 
                        type="range" 
                        min="0" max="1" step="0.01" 
                        value={val}
                        onChange={(e) => handleWeightChange(key as keyof ScenarioConfig['weights'], parseFloat(e.target.value))}
                        className="w-full accent-purple-500 h-1.5 bg-[var(--border)] rounded-full appearance-none outline-none cursor-pointer"
                      />
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-[var(--border)] flex justify-end gap-3 bg-[rgba(5,11,30,0.9)] shrink-0">
          <button 
            onClick={onClose}
            className="px-4 py-2 text-sm font-semibold text-[var(--muted)] hover:text-white transition-colors rounded hover:bg-white/5"
            disabled={loading}
          >
            Cancelar
          </button>
          <button 
            onClick={handleSave}
            disabled={loading}
            className="px-5 py-2 text-sm font-bold text-[#050b1e] bg-[var(--primary)] hover:bg-[#38bdf8] flex items-center gap-2 rounded shadow-[0_0_15px_rgba(56,189,248,0.4)] disabled:opacity-50 transition-all hover:scale-[1.02] active:scale-95"
          >
            {loading ? (
              <div className="w-4 h-4 border-2 border-black border-t-transparent rounded-full animate-spin" />
            ) : (
              <Save size={16} />
            )}
            {loading ? "Recalculando..." : "Executar e Atualizar"}
          </button>
        </div>
      </div>
    </div>
  );
}
