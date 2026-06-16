package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	port := flag.String("port", "9001", "Backend server port")
	name := flag.String("name", "", "Backend name (defaults to port)")
	delay := flag.Duration("delay", 0, "Response delay")
	unhealthy := flag.Bool("unhealthy", false, "Start in unhealthy state")
	failOn := flag.String("fail-on", "", "If set, return 503 when path contains this substring (for retry tests)")
	flag.Parse()

	backendName := *name
	if backendName == "" {
		backendName = "backend-" + *port
	}

	isUnhealthy := *unhealthy

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if isUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/toggle-health", func(w http.ResponseWriter, r *http.Request) {
		isUnhealthy = !isUnhealthy
		status := "healthy"
		if isUnhealthy {
			status = "unhealthy"
		}
		fmt.Fprintf(w, "Backend %s is now %s\n", backendName, status)
		fmt.Printf("[%s] Health toggled to: %s\n", backendName, status)
	})

	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		echoHandler(w, r, backendName, failOn, delay)
	})
	mux.HandleFunc("/echo/", func(w http.ResponseWriter, r *http.Request) {
		echoHandler(w, r, backendName, failOn, delay)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *failOn != "" && contains(r.URL.Path, *failOn) {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "Backend %s intentionally failing\n", backendName)
			fmt.Printf("[%s] Intentionally returning 503 for %s\n", backendName, r.URL.Path)
			return
		}
		if *delay > 0 {
			time.Sleep(*delay)
		}
		w.Header().Set("X-Backend", backendName)
		fmt.Fprintf(w, "Hello from %s\n", backendName)
		fmt.Fprintf(w, "Path: %s\n", r.URL.Path)
		fmt.Fprintf(w, "Method: %s\n", r.Method)
		fmt.Fprintf(w, "X-Session-Id: %s\n", r.Header.Get("X-Session-Id"))
	})

	addr := ":" + *port
	fmt.Printf("Backend %s starting on %s (healthy: %v)\n", backendName, addr, !isUnhealthy)

	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Printf("Backend error: %v\n", err)
		os.Exit(1)
	}
}

func echoHandler(w http.ResponseWriter, r *http.Request, backendName string, failOn *string, delay *time.Duration) {
	if *failOn != "" && contains(r.URL.Path, *failOn) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, "Backend %s intentionally failing\n", backendName)
		fmt.Printf("[%s] Intentionally returning 503 for %s\n", backendName, r.URL.Path)
		return
	}
	if *delay > 0 {
		time.Sleep(*delay)
	}

	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Backend", backendName)

	resp := map[string]interface{}{
		"backend":      backendName,
		"method":       r.Method,
		"path":         r.URL.Path,
		"content_type": r.Header.Get("Content-Type"),
		"content_len":  len(body),
		"body":         string(body),
		"session_id":   r.Header.Get("X-Session-Id"),
	}
	_ = json.NewEncoder(w).Encode(resp)
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
