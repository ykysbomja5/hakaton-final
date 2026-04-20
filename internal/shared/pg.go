package shared

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func OpenPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, nil
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 8
	cfg.MinConns = 1
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 15 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
