// Score → RGBA color ramp for Deck.gl layers
// 0=purple → 25=blue → 50=teal → 75=amber → 100=red

type RGBA = [number, number, number, number];

const STOPS: { at: number; color: RGBA }[] = [
  { at: 0,   color: [80,   0,   220, 210] },
  { at: 25,  color: [0,   110,  255, 210] },
  { at: 50,  color: [0,   210,  200, 210] },
  { at: 75,  color: [255, 180,    0, 210] },
  { at: 100, color: [255,  50,    0, 210] },
];

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

export function scoreToColor(score: number | null, alpha?: number): RGBA {
  const s = Math.max(0, Math.min(100, score ?? 0));

  let lo = STOPS[0];
  let hi = STOPS[STOPS.length - 1];

  for (let i = 0; i < STOPS.length - 1; i++) {
    if (s >= STOPS[i].at && s <= STOPS[i + 1].at) {
      lo = STOPS[i];
      hi = STOPS[i + 1];
      break;
    }
  }

  const t = lo.at === hi.at ? 0 : (s - lo.at) / (hi.at - lo.at);
  return [
    Math.round(lerp(lo.color[0], hi.color[0], t)),
    Math.round(lerp(lo.color[1], hi.color[1], t)),
    Math.round(lerp(lo.color[2], hi.color[2], t)),
    alpha ?? lo.color[3],
  ];
}

// CSS hex equivalent for UI elements (tier badges, score bars)
export const TIER_COLORS: Record<string, string> = {
  A: '#22c55e',  // green-500
  B: '#38bdf8',  // sky-400
  C: '#f59e0b',  // amber-500
  D: '#ef4444',  // red-500
};

export function tierColor(tier: string | null): string {
  return TIER_COLORS[tier ?? ''] ?? '#64748b';
}
