package main

import (
	"log"
	"net/http"
	"os"

	"github.com/sumi-studio/sumi/apps/api/internal/handler"
	"github.com/sumi-studio/sumi/apps/api/internal/store"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	controlPlanePath := os.Getenv("SUMI_CONTROL_PLANE_STATE_PATH")
	if controlPlanePath == "" {
		controlPlanePath = "/var/lib/sumi-control-plane/tombstones.json"
	}
	tombstoneStore, err := store.OpenTombstoneStore(controlPlanePath)
	if err != nil {
		log.Fatalf("open persistent control-plane tombstones: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)
	handler.RegisterTombstoneRoutes(mux, tombstoneStore)

	log.Printf("sumi api listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
