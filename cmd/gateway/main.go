package main

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"drivee-self-service/internal/shared"
)

func main() {
	port := getenv("PORT", "8080")
	queryURL := mustParseURL(getenv("QUERY_SERVICE_URL", "http://localhost:8081"))
	reportsURL := mustParseURL(getenv("REPORTS_SERVICE_URL", "http://localhost:8083"))
	metaURL := mustParseURL(getenv("META_SERVICE_URL", "http://localhost:8084"))

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", proxy(queryURL))
	mux.Handle("/api/v1/query/", proxy(queryURL))
	mux.Handle("/api/v1/reports", proxy(reportsURL))
	mux.Handle("/api/v1/reports/", proxy(reportsURL))
	mux.Handle("/api/v1/meta", proxy(metaURL))
	mux.Handle("/api/v1/meta/", proxy(metaURL))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		shared.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "gateway"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		serveFrontend(w, r)
	})

	log.Printf("gateway listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, loggingMiddleware(mux)))
}

func proxy(target *url.URL) http.Handler {
	reverseProxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := reverseProxy.Director
	reverseProxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shared.HandlePreflight(w, r) {
			return
		}
		reverseProxy.ServeHTTP(w, r)
	})
}

func serveFrontend(w http.ResponseWriter, r *http.Request) {
	root := filepath.Join(".", "web")
	requested := filepath.Clean(strings.TrimPrefix(r.URL.Path, "/"))
	if requested == "." || requested == "" {
		http.ServeFile(w, r, filepath.Join(root, "index.html"))
		return
	}

	full := filepath.Join(root, requested)
	if info, err := os.Stat(full); err == nil && !info.IsDir() {
		http.ServeFile(w, r, full)
		return
	}

	http.ServeFile(w, r, filepath.Join(root, "index.html"))
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.Path)
		next.ServeHTTP(w, r)
	})
}

func mustParseURL(raw string) *url.URL {
	parsed, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid service url %q: %v", raw, err)
	}
	return parsed
}

func getenv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
