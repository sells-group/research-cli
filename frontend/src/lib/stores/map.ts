import { writable } from "svelte/store";
import type { Map as MaplibreMap } from "maplibre-gl";

export interface MapState {
  center: [number, number];
  zoom: number;
  bounds: [number, number, number, number] | null;
  baseLayer: "dark" | "light" | "satellite";
}

export const mapState = writable<MapState>({
  center: [-98.5795, 39.8283],
  zoom: 4,
  bounds: null,
  baseLayer: "dark",
});

export const mapInstance = writable<MaplibreMap | null>(null);

export const flyTarget = writable<{
  lng: number;
  lat: number;
  zoom: number;
} | null>(null);
