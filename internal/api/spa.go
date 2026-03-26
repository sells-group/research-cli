package api

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// SPAHandler serves the SvelteKit frontend build, falling back to index.html for client-side routing.
func SPAHandler(buildDir string) http.Handler {
	fs := http.FileServer(http.Dir(buildDir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the file directly.
		path, ok := spaAssetPath(buildDir, r.URL.Path)
		if ok {
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				fs.ServeHTTP(w, r)
				return
			}
		}
		// Fall back to index.html for SPA routing.
		http.ServeFile(w, r, filepath.Join(buildDir, "index.html"))
	})
}

func spaAssetPath(buildDir, requestPath string) (string, bool) {
	clean := filepath.Clean("/" + requestPath)
	relative := strings.TrimPrefix(clean, string(filepath.Separator))
	path := filepath.Join(buildDir, relative)
	relToRoot, err := filepath.Rel(buildDir, path)
	if err != nil {
		return "", false
	}
	if relToRoot == ".." || strings.HasPrefix(relToRoot, ".."+string(filepath.Separator)) {
		return "", false
	}
	return path, true
}
