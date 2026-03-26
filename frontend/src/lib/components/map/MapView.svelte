<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { mapState, mapInstance, flyTarget } from '$lib/stores/map';
  import { selectedCompany } from '$lib/stores/companies';
  import { activeView } from '$lib/stores/view';
  import { layerState } from '$lib/stores/layers';
  import { LAYERS } from '$lib/config/layer-defs';
  import type { LayerDef } from '$lib/config/layer-defs';
  import * as Card from '$lib/components/ui/card';
  import LoadingSpinner from '$lib/components/shared/LoadingSpinner.svelte';
  import MapFilterPanel from './MapFilterPanel.svelte';
  import maplibregl from 'maplibre-gl';
  import { Protocol } from 'pmtiles';
  import { layers as protoLayers, namedFlavor } from '@protomaps/basemaps';
  import { mode } from 'mode-watcher';

  let mapContainer: HTMLDivElement;
  let map: maplibregl.Map;
  let loading = $state(true);
  let protocol: Protocol;
  let currentFlavor: 'dark' | 'light' = 'dark';
  let basemapLayerIds: string[] = [];
  let mapLoaded = false;
  let activeLayerKeys = $state<Record<string, boolean>>({});

  // Company filter state (passed to MapFilterPanel)
  let scoreMin = $state(0);
  let scoreMax = $state(100);
  let stateFilter = $state('');

  // Track current base layer
  let currentBase = $state<'dark' | 'light' | 'satellite'>('dark');

  const TILE_URL = `${typeof window !== 'undefined' ? window.location.origin : ''}/tiles`;

  function swapBasemap(m: maplibregl.Map, flavor: 'dark' | 'light') {
    if (flavor === currentFlavor && basemapLayerIds.length > 0) return;

    const allLayers = m.getStyle().layers ?? [];
    let firstDataLayer: string | undefined;
    for (const l of allLayers) {
      if (!basemapLayerIds.includes(l.id)) {
        firstDataLayer = l.id;
        break;
      }
    }

    for (const id of basemapLayerIds) {
      if (m.getLayer(id)) m.removeLayer(id);
    }

    const newLayers = protoLayers('protomaps', namedFlavor(flavor), { lang: 'en' });
    basemapLayerIds = newLayers.map((l: any) => l.id);
    for (const layer of newLayers) {
      m.addLayer(layer as any, firstDataLayer);
    }

    currentFlavor = flavor;
  }

  // Get the MapLibre layer ID for a LayerDef
  function mlLayerId(def: LayerDef): string {
    return `layer-${def.key}`;
  }

  // Get outline layer ID for fill layers
  function mlOutlineId(def: LayerDef): string {
    return `layer-${def.key}-outline`;
  }

  // Add MVT source for a layer
  function addMVTSource(m: maplibregl.Map, def: LayerDef) {
    const sourceId = `src-${def.key}`;
    if (m.getSource(sourceId)) return;
    m.addSource(sourceId, {
      type: 'vector',
      tiles: [`${TILE_URL}/${def.key}/{z}/{x}/{y}.pbf`],
      minzoom: def.minZoom,
      maxzoom: def.maxZoom,
    });
  }

  // Add MapLibre style layer for a LayerDef
  function addStyleLayer(m: maplibregl.Map, def: LayerDef) {
    const sourceId = `src-${def.key}`;
    const layerId = mlLayerId(def);

    if (m.getLayer(layerId)) return;

    if (def.type === 'fill') {
      m.addLayer({
        id: layerId,
        type: 'fill',
        source: sourceId,
        'source-layer': def.key,
        minzoom: def.minZoom,
        maxzoom: def.maxZoom,
        paint: {
          'fill-color': def.color,
          'fill-opacity': def.opacity ?? 0.15,
        },
        layout: { visibility: 'none' },
      });
      // Add outline for polygon layers
      m.addLayer({
        id: mlOutlineId(def),
        type: 'line',
        source: sourceId,
        'source-layer': def.key,
        minzoom: def.minZoom,
        maxzoom: def.maxZoom,
        paint: {
          'line-color': def.color,
          'line-opacity': 0.4,
          'line-width': 0.5,
        },
        layout: { visibility: 'none' },
      });
    } else if (def.type === 'line') {
      m.addLayer({
        id: layerId,
        type: 'line',
        source: sourceId,
        'source-layer': def.key,
        minzoom: def.minZoom,
        maxzoom: def.maxZoom,
        paint: {
          'line-color': def.color,
          'line-width': def.width ?? 1.5,
          'line-opacity': 0.8,
        },
        layout: { visibility: 'none' },
      });
    } else if (def.type === 'circle') {
      // Special handling for companies: score-based color
      const isCompany = def.key === 'companies';
      m.addLayer({
        id: layerId,
        type: 'circle',
        source: sourceId,
        'source-layer': def.key,
        minzoom: def.minZoom,
        maxzoom: def.maxZoom,
        paint: {
          'circle-color': isCompany
            ? [
                'interpolate', ['linear'],
                ['coalesce', ['get', 'score'], 0],
                0, '#ef4444',
                50, '#eab308',
                100, '#22c55e',
              ]
            : def.color,
          'circle-radius': def.radius ?? 4,
          'circle-stroke-width': isCompany ? 1.5 : 0.5,
          'circle-stroke-color': 'rgba(255,255,255,0.6)',
          'circle-opacity': 0.9,
        },
        layout: { visibility: 'none' },
      });
    }
  }

  // Set layer visibility
  function setLayerVisibility(m: maplibregl.Map, def: LayerDef, visible: boolean) {
    const vis = visible ? 'visible' : 'none';
    const layerId = mlLayerId(def);
    if (m.getLayer(layerId)) {
      m.setLayoutProperty(layerId, 'visibility', vis);
    }
    if (def.type === 'fill') {
      const outlineId = mlOutlineId(def);
      if (m.getLayer(outlineId)) {
        m.setLayoutProperty(outlineId, 'visibility', vis);
      }
    }
  }

  // Build popup HTML for a feature
  function buildPopupHTML(def: LayerDef, props: Record<string, any>, _lngLat: maplibregl.LngLat): string {
    if (def.key === 'companies') {
      const scoreVal = props.score != null ? Math.round(props.score) : null;
      const scoreBadge = scoreVal != null
        ? `<span class="score-badge" style="background:${scoreVal >= 70 ? '#22c55e' : scoreVal >= 40 ? '#eab308' : '#ef4444'}">${scoreVal}</span>`
        : '';
      return `
        <div class="popup-content">
          <div class="popup-header">
            <strong>${props.name ?? 'Unknown'}</strong>
            ${scoreBadge}
          </div>
          ${props.domain ? `<div class="popup-domain">${props.domain}</div>` : ''}
          ${props.city || props.state ? `<div class="popup-location">${[props.city, props.state].filter(Boolean).join(', ')}</div>` : ''}
          <button class="popup-detail-btn" onclick="window.__mapViewDetail(${props.company_id})">View Detail</button>
        </div>`;
    }

    // Generic popup for other layers
    const title = props.name || props.zone_code || def.label;
    const details = Object.entries(props)
      .filter(([k]) => !['id', 'name', 'geom'].includes(k))
      .slice(0, 5)
      .map(([k, v]) => `<div class="text-xs text-muted-foreground">${k}: ${v}</div>`)
      .join('');
    return `
      <div class="popup-content">
        <strong>${title}</strong>
        ${details}
      </div>`;
  }

  function handleApplyFilters(_filters: { scoreMin: number; scoreMax: number; state: string }) {
    if (!map || !mapLoaded) return;
    const layerId = 'layer-companies';
    if (!map.getLayer(layerId)) return;

    // Use filter expressions to show/hide based on score
    const filter: any[] = ['all'];
    if (_filters.scoreMin > 0) filter.push(['>=', ['coalesce', ['get', 'score'], 0], _filters.scoreMin]);
    if (_filters.scoreMax < 100) filter.push(['<=', ['coalesce', ['get', 'score'], 0], _filters.scoreMax]);
    if (_filters.state) {
      const states = _filters.state.split(',').map(s => s.trim().toUpperCase());
      if (states.length === 1) {
        filter.push(['==', ['upcase', ['coalesce', ['get', 'state'], '']], states[0]]);
      } else {
        filter.push(['in', ['upcase', ['coalesce', ['get', 'state'], '']], ['literal', states]]);
      }
    }

    map.setFilter(layerId, filter.length > 1 ? filter : null);
  }

  let unsubFly: (() => void) | null = null;
  let unsubLayers: (() => void) | null = null;

  onMount(() => {
    protocol = new Protocol();
    maplibregl.addProtocol('pmtiles', protocol.tile);

    const initialFlavor: 'dark' | 'light' = mode.current === 'light' ? 'light' : 'dark';
    currentFlavor = initialFlavor;
    currentBase = initialFlavor;
    const initialLayers = protoLayers('protomaps', namedFlavor(initialFlavor), { lang: 'en' });
    basemapLayerIds = initialLayers.map((l: any) => l.id);

    map = new maplibregl.Map({
      container: mapContainer,
      style: {
        version: 8,
        glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
        sprite: `https://protomaps.github.io/basemaps-assets/sprites/v4/${initialFlavor}`,
        sources: {
          protomaps: {
            type: 'vector',
            url: 'pmtiles://https://build.protomaps.com/20250305.pmtiles',
            attribution: '&copy; <a href="https://openstreetmap.org">OpenStreetMap</a>',
          },
        },
        layers: initialLayers as any[],
      },
      center: [-98.5, 39.8],
      zoom: 4,
    });

    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    map.addControl(new maplibregl.ScaleControl(), 'bottom-left');
    mapInstance.set(map);

    // Tooltip (hover)
    const tooltip = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 12,
      className: 'feature-tooltip',
    });

    // Popup (click)
    const popup = new maplibregl.Popup({
      closeButton: true,
      maxWidth: '300px',
      className: 'feature-popup',
    });

    map.on('load', () => {
      mapLoaded = true;

      // Add all MVT sources and layers
      for (const def of LAYERS) {
        addMVTSource(map, def);
        addStyleLayer(map, def);
      }

      // Set initial visibility from store
      const currentState = activeLayerKeys;
      for (const def of LAYERS) {
        setLayerVisibility(map, def, currentState[def.key] ?? false);
      }

      // Hover handlers for all circle/point layers
      for (const def of LAYERS) {
        if (def.type !== 'circle') continue;
        const layerId = mlLayerId(def);

        map.on('mouseenter', layerId, (e) => {
          map.getCanvas().style.cursor = 'pointer';
          const feature = e.features?.[0];
          if (!feature?.properties) return;
          const p = feature.properties;
          const name = p.name || def.label;
          tooltip.setLngLat(e.lngLat).setHTML(
            `<strong>${name}</strong>` +
            (p.city || p.state ? `<br/><span class="text-muted">${[p.city, p.state].filter(Boolean).join(', ')}</span>` : '')
          ).addTo(map);
        });

        map.on('mouseleave', layerId, () => {
          map.getCanvas().style.cursor = '';
          tooltip.remove();
        });

        map.on('click', layerId, (e) => {
          tooltip.remove();
          const feature = e.features?.[0];
          if (!feature?.properties) return;
          popup.setLngLat(e.lngLat).setHTML(
            buildPopupHTML(def, feature.properties, e.lngLat)
          ).addTo(map);
        });
      }

      // Also add click handler for fill layers (boundaries)
      for (const def of LAYERS) {
        if (def.type !== 'fill') continue;
        const layerId = mlLayerId(def);

        map.on('click', layerId, (e) => {
          const feature = e.features?.[0];
          if (!feature?.properties) return;
          popup.setLngLat(e.lngLat).setHTML(
            buildPopupHTML(def, feature.properties, e.lngLat)
          ).addTo(map);
        });
      }

      // View detail handler for company popups
      (window as any).__mapViewDetail = (companyId: number) => {
        popup.remove();
        selectedCompany.set({ id: companyId } as any);
        activeView.set('companies');
      };

      loading = false;
    });

    // Subscribe to layer state changes
    unsubLayers = layerState.subscribe(state => {
      activeLayerKeys = state;
      if (!map || !mapLoaded) return;
      for (const def of LAYERS) {
        setLayerVisibility(map, def, state[def.key] ?? false);
      }
    });

    // Fly-to handler
    unsubFly = flyTarget.subscribe(target => {
      if (target && map) {
        map.flyTo({ center: [target.lng, target.lat], zoom: target.zoom });
        flyTarget.set(null);
      }
    });

    return () => {
      unsubFly?.();
      unsubLayers?.();
    };
  });

  // Dark/light mode sync
  $effect(() => {
    const m = mode.current;
    if (!map || !mapLoaded || !m) return;
    const flavor = m === 'dark' ? 'dark' : 'light';
    if (currentBase !== 'satellite') {
      swapBasemap(map, flavor);
      currentBase = flavor;
    }
  });

  // Basemap switcher
  function setBasemap(base: 'dark' | 'light' | 'satellite') {
    if (!map || !mapLoaded) return;
    currentBase = base;
    mapState.update(s => ({ ...s, baseLayer: base }));

    if (base === 'satellite') {
      // Hide Protomaps vector basemap
      for (const id of basemapLayerIds) {
        if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
      }
      // Add satellite raster source if needed
      if (!map.getSource('satellite')) {
        map.addSource('satellite', {
          type: 'raster',
          tiles: [
            'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
          ],
          tileSize: 256,
          attribution: '&copy; Esri',
        });
      }
      if (!map.getLayer('satellite-tiles')) {
        // Insert satellite layer below all data layers
        const firstDataLayer = LAYERS.map(d => mlLayerId(d)).find(id => map.getLayer(id));
        map.addLayer({
          id: 'satellite-tiles',
          type: 'raster',
          source: 'satellite',
          paint: { 'raster-opacity': 1 },
        }, firstDataLayer);
      } else {
        map.setLayoutProperty('satellite-tiles', 'visibility', 'visible');
      }
    } else {
      // Hide satellite if present
      if (map.getLayer('satellite-tiles')) {
        map.setLayoutProperty('satellite-tiles', 'visibility', 'none');
      }
      // Show and swap Protomaps basemap
      for (const id of basemapLayerIds) {
        if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'visible');
      }
      swapBasemap(map, base);
    }
  }

  onDestroy(() => {
    if (protocol) maplibregl.removeProtocol('pmtiles');
    map?.remove();
    mapInstance.set(null);
  });
</script>

<div class="flex-1 relative">
  <div bind:this={mapContainer} class="absolute inset-0"></div>
  {#if loading}
    <div class="absolute inset-0 flex items-center justify-center bg-background/50 z-10">
      <LoadingSpinner label="Loading map..." />
    </div>
  {/if}
  <div class="absolute top-3 left-3 z-10 flex flex-col gap-2">
    <MapFilterPanel
      bind:scoreMin
      bind:scoreMax
      bind:stateFilter
      onApply={handleApplyFilters}
    />
  </div>

  <!-- Basemap switcher -->
  <div class="absolute bottom-8 right-3 z-10">
    <Card.Root class="flex gap-0.5 p-1">
      <button
        class="px-2 py-1 rounded text-[10px] font-medium transition-colors {currentBase === 'dark' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:text-foreground'}"
        onclick={() => setBasemap('dark')}
      >Dark</button>
      <button
        class="px-2 py-1 rounded text-[10px] font-medium transition-colors {currentBase === 'light' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:text-foreground'}"
        onclick={() => setBasemap('light')}
      >Light</button>
      <button
        class="px-2 py-1 rounded text-[10px] font-medium transition-colors {currentBase === 'satellite' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground hover:text-foreground'}"
        onclick={() => setBasemap('satellite')}
      >Satellite</button>
    </Card.Root>
  </div>
</div>

<style>
  :global(.feature-tooltip .maplibregl-popup-content) {
    background: var(--popover);
    color: var(--popover-foreground);
    padding: 6px 10px;
    border-radius: 4px;
    font-size: 12px;
    border: 1px solid var(--border);
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
  }
  :global(.feature-tooltip .maplibregl-popup-tip) {
    display: none;
  }
  :global(.feature-popup .maplibregl-popup-content) {
    background: var(--card);
    color: var(--card-foreground);
    padding: 12px;
    border-radius: 8px;
    font-size: 13px;
    border: 1px solid var(--border);
    box-shadow: 0 4px 12px rgba(0,0,0,0.4);
    max-width: 300px;
  }
  :global(.feature-popup .maplibregl-popup-close-button) {
    color: var(--muted-foreground);
    font-size: 16px;
    padding: 4px 8px;
  }
  :global(.popup-content) {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  :global(.popup-header) {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
  }
  :global(.score-badge) {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 20px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    color: #000;
  }
  :global(.popup-domain) {
    color: var(--muted-foreground);
    font-size: 12px;
  }
  :global(.popup-location) {
    color: var(--muted-foreground);
    font-size: 12px;
  }
  :global(.popup-detail-btn) {
    margin-top: 6px;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 500;
    background: var(--secondary);
    color: var(--secondary-foreground);
    border: none;
    cursor: pointer;
    text-align: center;
  }
  :global(.popup-detail-btn:hover) {
    opacity: 0.8;
  }
  :global(.text-muted) {
    color: var(--muted-foreground);
  }
</style>
