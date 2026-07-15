package main

import (
	"log"
	"net/http"
	"os"

	"github.com/sumi-studio/sumi/apps/api/internal/handler"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handler.Health)

	log.Printf("sumi api listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
