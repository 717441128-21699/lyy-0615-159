package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"loadbalancer/server"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	flag.Parse()

	lb, err := server.NewLoadBalancerServer(*configPath)
	if err != nil {
		fmt.Printf("Failed to create load balancer: %v\n", err)
		os.Exit(1)
	}

	if err := lb.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	fmt.Printf("Received signal %v, shutting down...\n", sig)
	lb.Stop()
	fmt.Println("Server stopped")
}
