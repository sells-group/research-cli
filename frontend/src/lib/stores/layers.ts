import { writable, derived } from "svelte/store";
import { LAYERS, type LayerGroup } from "$lib/config/layer-defs";

export type LayerState = Record<string, boolean>;

const STORAGE_KEY = "map-layer-state";

/** Load layer visibility state from localStorage, falling back to defaults. */
function loadState(): LayerState {
  const state: LayerState = {};
  for (const l of LAYERS) {
    state[l.key] = false;
  }
  // Defaults: companies and counties on
  state["companies"] = true;
  state["counties"] = true;

  if (typeof window !== "undefined") {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved);
        for (const key of Object.keys(state)) {
          if (key in parsed) state[key] = parsed[key];
        }
      }
    } catch {
      /* ignore malformed localStorage */
    }
  }
  return state;
}

/** Create the layer visibility store with persistence and group helpers. */
function createLayerStore() {
  const store = writable<LayerState>(loadState());

  store.subscribe((state) => {
    if (typeof window !== "undefined") {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
      } catch {
        /* ignore quota errors */
      }
    }
  });

  return {
    subscribe: store.subscribe,

    /** Toggle a single layer on/off. */
    toggle(key: string) {
      store.update((s) => ({ ...s, [key]: !s[key] }));
    },

    /** Set a single layer's visibility. */
    setVisible(key: string, visible: boolean) {
      store.update((s) => ({ ...s, [key]: visible }));
    },

    /** Set all layers in a group to the same visibility. */
    setGroupVisible(group: LayerGroup, visible: boolean) {
      store.update((s) => {
        const next = { ...s };
        for (const l of LAYERS) {
          if (l.group === group) next[l.key] = visible;
        }
        return next;
      });
    },
  };
}

/** Global layer visibility store, persisted to localStorage. */
export const layerState = createLayerStore();

/** Derived count of currently active layers. */
export const activeLayerCount = derived(
  { subscribe: layerState.subscribe },
  ($state) => Object.values($state).filter(Boolean).length,
);
