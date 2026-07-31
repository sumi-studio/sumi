package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/migrations"
)

func main() {
	databaseURL := os.Getenv("SUMI_DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("SUMI_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}
	if err := migrations.Run(context.Background(), pool); err != nil {
		log.Fatal(err)
	}
	log.Print("database migrations complete")
}
