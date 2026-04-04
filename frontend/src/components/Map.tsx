'use client';

import { useRef, useState, useCallback, useEffect, useMemo } from 'react';
import MapGL, { NavigationControl } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer, ArcLayer, ScatterplotLayer } from '@deck.gl/layers';
import { FlyToInterpolator } from '@deck.gl/core';
import type { PickingInfo } from '@deck.gl/core';
import type { FeatureCollection } from 'geojson';
import 'maplibre-gl/dist/maplibre-gl.css';

import type { Municipio, TradeAreaItem } from '@/lib/types';
import { scoreToColor } from '@/lib/colors';

// ── Local types ────────────────────────────────────────────────────────────────
type GeoProps = Record<string, string | number | null | undefined>;
type GeoFeature = { type: 'Feature'; geometry: { type: string; coordinates: unknown } | null; properties: GeoProps | null };
type GeoFC = { type: 'FeatureCollection'; features: GeoFeature[] };

interface HoverInfo { x: number; y: number; object: Municipio }
interface TradeArcDatum { from: [number, number]; to: [number, number]; probability: number }
interface ViewState {
  longitude: number; latitude: number; zoom: number;
  pitch?: number; bearing?: number;
  transitionDuration?: number | 'auto';
  transitionInterpolator?: FlyToInterpolator;
}

interface Props {
  municipalities: Municipio[];
  selectedId: string | null;
  uf: string;
  tradeAreaData: { center: [number, number]; items: TradeAreaItem[] } | null;
  onCityClick: (codigoIbge: string) => void;
}

// ── Constants ──────────────────────────────────────────────────────────────────
const BRAZIL_CENTER: ViewState = { longitude: -50, latitude: -14, zoom: 4 };
const MAP_STYLE = 'https://tiles.openfreemap.org/styles/positron';
const IBGE = 'https://servicodados.ibge.gov.br/api/v3/malhas';

// ── Geometry helpers ───────────────────────────────────────────────────────────
function flatCoords(val: unknown): [number, number][] {
  if (!Array.isArray(val)) return [];
  if (typeof val[0] === 'number') return [val as [number, number]];
  return (val as unknown[]).flatMap(flatCoords);
}

function featuresBbox(features: GeoFeature[]): [[number, number], [number, number]] | null {
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  for (const f of features) {
    if (!f.geometry) continue;
    for (const [lng, lat] of flatCoords(f.geometry.coordinates)) {
      if (lng < w) w = lng;
      if (lat < s) s = lat;
      if (lng > e) e = lng;
      if (lat > n) n = lat;
    }
  }
  return isFinite(w) ? [[w, s], [e, n]] : null;
}

// Mercator fit-bounds: returns {longitude, latitude, zoom} to show bbox in viewport
function fitViewState(
  bbox: [[number, number], [number, number]],
  width: number,
  height: number,
  padding = 52,
): { longitude: number; latitude: number; zoom: number } {
  const [[west, south], [east, north]] = bbox;
  const longitude = (west + east) / 2;
  const latitude = (south + north) / 2;
  const pw = Math.max(1, width - padding * 2);
  const ph = Math.max(1, height - padding * 2);
  // 512 px = full world at zoom 0 (maplibre tile size)
  const lngFrac = (east - west) / 360;
  const mercN = Math.log(Math.tan(Math.PI / 4 + north * Math.PI / 360));
  const mercS = Math.log(Math.tan(Math.PI / 4 + south * Math.PI / 360));
  const latFrac = Math.abs(mercN - mercS) / (2 * Math.PI);
  const zoomLng = lngFrac > 0 ? Math.log2(pw / (512 * lngFrac)) : 20;
  const zoomLat = latFrac > 0 ? Math.log2(ph / (512 * latFrac)) : 20;
  return { longitude, latitude, zoom: Math.min(zoomLng, zoomLat) };
}

// ── Component ──────────────────────────────────────────────────────────────────
export default function PharmaSiteMap({ municipalities, selectedId, uf, tradeAreaData, onCityClick }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef(null);
  const brazilCache = useRef<GeoFC | null>(null);
  const loadedUfRef = useRef<string>(''); // tracks which UF mesh is currently in geoData

  const [viewState, setViewState] = useState<ViewState>(BRAZIL_CENTER);
  const [hoverInfo, setHoverInfo] = useState<HoverInfo | null>(null);
  const [geoData, setGeoData] = useState<GeoFC | null>(null);
  const [geoLoading, setGeoLoading] = useState(false);

  // Score lookup: codigo_ibge → Municipio
  const scoreMap = useMemo(
    () => new Map(municipalities.map(m => [m.codigo_ibge, m])),
    [municipalities],
  );

  // Fit map view to a set of GeoJSON features
  const flyToBbox = useCallback((features: GeoFeature[], maxZoom = 14) => {
    const bbox = featuresBbox(features);
    if (!bbox || !containerRef.current) return;
    const { offsetWidth: w, offsetHeight: h } = containerRef.current;
    const { longitude, latitude, zoom } = fitViewState(bbox, w, h);
    setViewState(prev => ({
      ...prev,
      longitude,
      latitude,
      zoom: Math.min(zoom, maxZoom),
      transitionDuration: 1200,
      transitionInterpolator: new FlyToInterpolator({ speed: 1.5 }),
    }));
  }, []);

  // Fetch an IBGE malhas GeoJSON URL
  const loadMesh = useCallback((url: string, onLoaded?: (fc: GeoFC) => void) => {
    setGeoLoading(true);
    fetch(url)
      .then(r => { if (!r.ok) throw new Error(`IBGE ${r.status}`); return r.json() as Promise<GeoFC>; })
      .then(fc => { setGeoData(fc); onLoaded?.(fc); })
      .catch(console.error)
      .finally(() => setGeoLoading(false));
  }, []);

  // React to UF filter changes
  useEffect(() => {
    if (!uf) {
      loadedUfRef.current = '';
      setGeoData(null); // Force scatter fallback for national view
      setViewState(prev => ({
        ...prev, ...BRAZIL_CENTER,
        transitionDuration: 1000,
        transitionInterpolator: new FlyToInterpolator({ speed: 1.2 }),
      }));
      return;
    }
    loadedUfRef.current = uf;
    loadMesh(
      `${IBGE}/estados/${uf}?formato=application/vnd.geo+json&resolucao=5&intrarregiao=municipio`,
      fc => { loadedUfRef.current = uf; flyToBbox(fc.features); },
    );
  }, [uf, loadMesh, flyToBbox]);

  // Fly to selected municipality — zoom to entire state, not just the city
  useEffect(() => {
    if (!selectedId) return;
    if (geoData) {
      const feat = geoData.features.find(f => f.properties?.codarea === selectedId);
      if (feat) {
        // Zoom out to encompass the whole state mesh (all features loaded)
        flyToBbox(geoData.features);
        return;
      }
    }
    // National view: selected city not in the current mesh — fetch its state mesh
    const m = scoreMap.get(selectedId);
    if (m?.uf && m.uf !== loadedUfRef.current) {
      loadMesh(
        `${IBGE}/estados/${m.uf}?formato=application/vnd.geo+json&resolucao=5&intrarregiao=municipio`,
        fc => { loadedUfRef.current = m.uf; flyToBbox(fc.features); },
      );
      return;
    }
    // Final fallback: center on lat/lon point
    if (m?.latitude && m?.longitude) {
      setViewState(prev => ({
        ...prev,
        longitude: m.longitude!,
        latitude: m.latitude!,
        zoom: 11,
        transitionDuration: 1000,
        transitionInterpolator: new FlyToInterpolator({ speed: 1.5 }),
      }));
    }
  }, [selectedId, geoData, scoreMap, flyToBbox, loadMesh]);

  // ── Layers ─────────────────────────────────────────────────────────────────
  const geoLayer = geoData
    ? new GeoJsonLayer({
      id: 'municipalities-geo',
      data: geoData as unknown as FeatureCollection,
      filled: true,
      stroked: true,
      getFillColor: (f: unknown) => {
        const cod = (f as GeoFeature).properties?.codarea as string | undefined;
        const m = cod ? scoreMap.get(cod) : undefined;
        if (!m) return [10, 25, 65, 55];                   // no API data: dark navy
        if (m.score_total === null) return [100, 116, 139, 150]; // no score: slate-grey
        return scoreToColor(m.score_total, 200);
      },
      getLineColor: (f: unknown) =>
        (f as GeoFeature).properties?.codarea === selectedId
          ? [255, 255, 255, 255]           // selected city: bright white
          : uf
            ? [56, 189, 248, 80]           // state view: light cyan borders
            : [10, 25, 65, 110],           // national view: dark navy borders
      lineWidthUnits: 'pixels' as const,
      getLineWidth: (f: unknown) =>
        (f as GeoFeature).properties?.codarea === selectedId ? 2.5 : 0.4,
      lineWidthMinPixels: 0.3,
      pickable: true,
      autoHighlight: false,
      onClick: (info: PickingInfo) => {
        const cod = (info.object as GeoFeature | null)?.properties?.codarea as string | undefined;
        if (cod && scoreMap.has(cod)) onCityClick(cod);
      },
      onHover: (info: PickingInfo) => {
        const cod = (info.object as GeoFeature | null)?.properties?.codarea as string | undefined;
        const m = cod ? scoreMap.get(cod) : undefined;
        setHoverInfo(m ? { x: info.x, y: info.y, object: m } : null);
      },
      updateTriggers: {
        getFillColor: scoreMap,
        getLineColor: [selectedId, uf],
        getLineWidth: selectedId,
      },
    })
    : null;

  // Scatter fallback while GeoJSON is loading (or failed to load)
  const scatterFallback = !geoData
    ? new ScatterplotLayer<Municipio>({
      id: 'municipalities-scatter',
      data: municipalities.filter(m => m.latitude !== null && m.longitude !== null),
      getPosition: d => [d.longitude!, d.latitude!],
      getRadius: d => (d.codigo_ibge === selectedId ? 9000 : 6000),
      getFillColor: d => d.score_total === null ? [100, 116, 139, 150] : scoreToColor(d.score_total),
      getLineColor: d => d.codigo_ibge === selectedId ? [255, 255, 255, 255] : [255, 255, 255, 80],
      getLineWidth: d => (d.codigo_ibge === selectedId ? 3 : 1),
      lineWidthMinPixels: 1,
      radiusMinPixels: 3,
      radiusMaxPixels: 18,
      pickable: true,
      onClick: (info: PickingInfo<Municipio>) => { if (info.object) onCityClick(info.object.codigo_ibge); },
      onHover: (info: PickingInfo<Municipio>) => {
        setHoverInfo(info.object ? { x: info.x, y: info.y, object: info.object } : null);
      },
      updateTriggers: { getRadius: selectedId, getLineColor: selectedId, getLineWidth: selectedId },
    })
    : null;

  // Trade area arcs and bubbles
  const arcData: TradeArcDatum[] = tradeAreaData
    ? tradeAreaData.items
      .filter(i => i.latitude !== null && i.longitude !== null)
      .map(i => ({ from: tradeAreaData.center, to: [i.longitude!, i.latitude!], probability: i.probability }))
    : [];

  const arcLayer = new ArcLayer<TradeArcDatum>({
    id: 'trade-arcs',
    data: arcData,
    getSourcePosition: d => d.from,
    getTargetPosition: d => d.to,
    getSourceColor: [56, 189, 248, 120],  // Less opacity
    getTargetColor: [255, 150, 0, 120],   // Less opacity
    getWidth: d => Math.max(1, d.probability * 2), // Much thinner arcs
    widthMinPixels: 1,
    widthMaxPixels: 2,
  });

  const tradeScatterLayer = new ScatterplotLayer<TradeAreaItem>({
    id: 'trade-bubbles',
    data: tradeAreaData?.items.filter(i => i.latitude !== null && i.longitude !== null) ?? [],
    getPosition: d => [d.longitude!, d.latitude!],
    getRadius: d => 1500 + d.probability * 4500, // Reduced from 30000m to 6000m 
    getFillColor: d => [255, 150, 0, Math.round(d.probability * 150)], // Reduced opacity
    getLineColor: [255, 200, 0, 180],
    getLineWidth: 1,
    lineWidthMinPixels: 1,
    radiusMinPixels: 2,
  });

  const layers = [
    ...(geoLayer ? [geoLayer] : scatterFallback ? [scatterFallback] : []),
    ...(tradeAreaData ? [tradeScatterLayer, arcLayer] : []),
  ];

  const handleViewStateChange = useCallback(
    ({ viewState: vs }: { viewState: ViewState }) => setViewState(vs),
    [],
  );

  return (
    <div ref={containerRef} className="relative w-full h-full">

      {/* IBGE mesh loading indicator */}
      {geoLoading && (
        <div className="absolute top-3 left-1/2 -translate-x-1/2 z-30 glass px-3 py-1.5 text-xs flex items-center gap-2">
          <div className="w-3.5 h-3.5 border-2 border-[var(--primary)] border-t-transparent rounded-full animate-spin" />
          Carregando malhas IBGE…
        </div>
      )}

      <DeckGL
        viewState={viewState}
        onViewStateChange={handleViewStateChange as (args: unknown) => void}
        controller={true}
        layers={layers}
        style={{ position: 'absolute', inset: '0' }}
        getCursor={({ isHovering }) => (isHovering ? 'pointer' : 'grab')}
      >
        <MapGL ref={mapRef} mapStyle={MAP_STYLE} attributionControl={false} reuseMaps>
          <NavigationControl position="bottom-right" />
        </MapGL>
      </DeckGL>

      {/* Hover tooltip */}
      {hoverInfo && (
        <div
          className="pointer-events-none absolute z-50 glass px-3 py-2 text-sm"
          style={{ left: hoverInfo.x + 14, top: hoverInfo.y - 10 }}
        >
          <div className="font-semibold text-white">{hoverInfo.object.nome}</div>
          <div className="text-[var(--text-dim)] text-xs">
            {hoverInfo.object.uf}
            {hoverInfo.object.tier && (
              <span className="ml-2 font-bold">Tier {hoverInfo.object.tier}</span>
            )}
          </div>
          <div className="text-[var(--primary)] font-mono text-xs mt-0.5">
            Score: {hoverInfo.object.score_total != null ? hoverInfo.object.score_total.toFixed(1) : 'N/A'}
          </div>
        </div>
      )}

      {/* Trade area legend */}
      {tradeAreaData && (
        <div className="absolute bottom-16 right-4 glass p-3 text-xs w-48">
          <div className="font-semibold mb-1 text-[var(--primary)]">Área de Influência</div>
          <div className="flex items-center gap-2 text-[var(--text-dim)]">
            <div className="w-3 h-3 rounded-full bg-[#38bdf8]" />
            Origem
          </div>
          <div className="flex items-center gap-2 text-[var(--text-dim)] mt-1">
            <div className="w-3 h-3 rounded-full bg-amber-400" />
            Atração proporcional
          </div>
        </div>
      )}
    </div>
  );
}
