// Command jsql is a test-fixture helper: run one SQL statement against
// a postgres DSN and print tab-separated rows — a minimal stand-in for
// psql inside fixtures that only ship Go.
//
//	jsql <dsn> <sql>
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: jsql <dsn> <sql>")
		os.Exit(2)
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		parts := make([]string, len(vals))
		for i, v := range vals {
			switch t := v.(type) {
			case nil:
				parts[i] = ""
			case []byte:
				parts[i] = string(t)
			default:
				parts[i] = fmt.Sprint(t)
			}
		}
		fmt.Println(strings.Join(parts, "\t"))
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
