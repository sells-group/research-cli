import { writable, get } from "svelte/store";

export type ActiveView =
  | "dashboard"
  | "enrichment"
  | "companies"
  | "fed-data"
  | "map"
  | "analytics";

const VALID_VIEWS = new Set<string>([
  "dashboard",
  "enrichment",
  "companies",
  "fed-data",
  "map",
  "analytics",
]);

/** Parse the current hash into view + optional sub-path segments. */
function parseHash(): { view: ActiveView; subPath: string[] } {
  const hash =
    typeof window !== "undefined"
      ? window.location.hash.replace(/^#\/?/, "")
      : "";
  if (!hash) return { view: "dashboard", subPath: [] };
  const parts = hash.split("/");
  const view = parts[0];
  if (VALID_VIEWS.has(view)) {
    return { view: view as ActiveView, subPath: parts.slice(1) };
  }
  return { view: "dashboard", subPath: [] };
}

function createViewStore() {
  const initial = parseHash();
  const { subscribe, set } = writable<ActiveView>(initial.view);

  return {
    subscribe,
    /** Set the active view and update the URL hash. */
    set(view: ActiveView) {
      set(view);
      pushHash(view);
    },
    /** Initialize from hash and listen for popstate. Call once on mount. */
    init() {
      const { view } = parseHash();
      set(view);

      window.addEventListener("popstate", () => {
        const { view } = parseHash();
        set(view);
      });
    },
  };
}

export const activeView = createViewStore();

/** Read the sub-path segments after the view name in the hash. */
export function getHashSubPath(): string[] {
  return parseHash().subPath;
}

/** Push a new hash entry into browser history. */
export function pushHash(path: string) {
  const newHash = `#${path}`;
  if (window.location.hash !== newHash) {
    history.pushState(null, "", newHash);
  }
}

/** Replace the current hash entry (no new history entry). */
export function replaceHash(path: string) {
  const newHash = `#${path}`;
  if (window.location.hash !== newHash) {
    history.replaceState(null, "", newHash);
  }
}
