package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	port := flag.String("port", "9001", "Backend server port")
	name := flag.String("name", "", "Backend name (defaults to port)")
	delay := flag.Duration("delay", 0, "Response delay")
	unhealthy := flag.Bool("unhealthy", false, "Start in unhealthy state")
	flag.Parse()

	backendName := *name
	if backendName == "" {
		backendName = "backend-" + *port
	}

	isUnhealthy := *unhealthy

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if isUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("unhealthy"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	http.HandleFunc("/toggle-health", func(w http.ResponseWriter, r *http.Request) {
		isUnhealthy = !isUnhealthy
		status := "healthy"
		if isUnhealthy {
			status = "unhealthy"
		}
		fmt.Fprintf(w, "Backend %s is now %s\n", backendName, status)
		fmt.Printf("[%s] Health toggled to: %s\n", backendName, status)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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

	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("Backend error: %v\n", err)
		os.Exit(1)
	}
}
