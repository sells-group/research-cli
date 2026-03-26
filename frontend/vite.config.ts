import tailwindcss from "@tailwindcss/vite";
import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig, type Plugin } from "vite";

const apiPort = process.env.API_PORT || "8080";

// SvelteKit's fallback handler intercepts /api/* before vite's built-in proxy.
// This plugin registers an early middleware that lets /api requests pass through
// to the proxy instead of being caught by SvelteKit's SPA fallback.
function apiProxy(): Plugin {
  return {
    name: "api-proxy-bypass",
    configureServer(server) {
      server.middlewares.use((req, _res, next) => {
        if (req.url?.startsWith("/api") || req.url?.startsWith("/tiles")) {
          // Remove accept header that triggers SvelteKit's HTML fallback.
          req.headers.accept = "application/json";
        }
        next();
      });
    },
  };
}

export default defineConfig({
  plugins: [apiProxy(), tailwindcss(), sveltekit()],
  server: {
    proxy: {
      "/api": {
        target: `http://localhost:${apiPort}`,
        changeOrigin: true,
      },
      "/tiles": {
        target: `http://localhost:${apiPort}`,
        changeOrigin: true,
      },
    },
  },
});
