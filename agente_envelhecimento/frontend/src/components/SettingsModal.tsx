import React, { useState, useEffect } from 'react';
import { Settings, Save, X } from 'lucide-react';
import { api } from '@/lib/api';
import type { ScenarioWeights } from '@/lib/types';

interface SettingsModalProps {
  onClose: () => void;
  onSaveConfig: () => void;
}

export default function SettingsModal({ onClose, onSaveConfig }: SettingsModalProps) {
  const [weights, setWeights] = useState<ScenarioWeights>({
    demo: 0.19,
    logistica: 0.40,
    economia: 0.22,
    saude: 0.10,
    competitividade: 0.08
  });
  const [loading, setLoading] = useState(false);
  const [fetching, setFetching] = useState(true);

  useEffect(() => {
    api.getActiveScenario()
      .then((res) => {
        if (res.weights) {
          setWeights(res.weights);
        }
      })
      .catch((err) => console.error("Failed to load scenario settings", err))
      .finally(() => setFetching(false));
  }, []);

  const handleChange = (key: keyof ScenarioWeights, val: number) => {
    setWeights(prev => ({ ...prev, [key]: val }));
  };

  const handleSave = async () => {
    setLoading(true);
    try {
      await api.saveScenario(weights);
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
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="glass w-[400px] border border-[var(--border)] rounded-xl overflow-hidden animate-in fade-in zoom-in-95 duration-200">
        
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-[var(--border)] bg-[rgba(5,11,30,0.8)]">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-[var(--primary-dim)] flex items-center justify-center border border-[rgba(56,189,248,0.3)]">
               <Settings size={16} className="text-[var(--primary)]" />
            </div>
            <div>
              <h3 className="text-sm font-bold text-white">Configuração do Motor</h3>
              <p className="text-[10px] text-[var(--muted)]">Ajuste os pesos dos pilares estruturais</p>
            </div>
          </div>
          <button onClick={onClose} className="text-[var(--muted)] hover:text-white transition-colors">
            <X size={18} />
          </button>
        </div>

        {/* Body */}
        <div className="p-5 flex flex-col gap-4 bg-[rgba(5,11,30,0.4)]">
          {Object.entries({
            'Logística (Proximidade)': 'logistica',
            'Economia (Renda)': 'economia',
            'Demografia (População)': 'demo',
            'Infraestrutura de Saúde': 'saude',
            'Competitividade (Concorrência)': 'competitividade'
          }).map(([label, key]) => {
            const val = weights[key as keyof ScenarioWeights] || 0;
            const pct = Math.round(val * 100);
            return (
              <div key={key} className="flex flex-col gap-1.5">
                <div className="flex justify-between text-xs font-semibold">
                  <span className="text-[var(--text-dim)]">{label}</span>
                  <span className="text-[var(--primary)]">{pct}%</span>
                </div>
                <input 
                  type="range" 
                  min="0" max="1" step="0.01" 
                  value={val}
                  onChange={(e) => handleChange(key as keyof ScenarioWeights, parseFloat(e.target.value))}
                  className="w-full accent-[var(--primary)] h-1.5 bg-[var(--border)] rounded-full appearance-none outline-none"
                />
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div className="p-4 border-t border-[var(--border)] flex justify-end gap-2 bg-[rgba(5,11,30,0.8)]">
          <button 
            onClick={onClose}
            className="px-4 py-2 text-xs font-semibold text-[var(--muted)] hover:text-white transition-colors"
            disabled={loading}
          >
            Cancelar
          </button>
          <button 
            onClick={handleSave}
            disabled={loading}
            className="px-4 py-2 text-xs font-bold text-[#050b1e] bg-[var(--primary)] hover:bg-[#38bdf8] flex items-center gap-1.5 rounded disabled:opacity-50 transition-colors"
          >
            {loading ? (
              <div className="w-3.5 h-3.5 border border-black border-t-transparent rounded-full animate-spin" />
            ) : (
              <Save size={14} />
            )}
            {loading ? "Recalculando Motor..." : "Salvar Cenário"}
          </button>
        </div>
      </div>
    </div>
  );
}
