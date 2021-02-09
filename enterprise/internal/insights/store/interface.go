package store

import "context"

// See https://github.com/efritz/go-mockgen
//go:generate go-mockgen -f github.com/sourcegraph/sourcegraph/enterprise/internal/insights/store -i Interface -o mock_store_interface.go

// Interface is the interface describing a code insights store. See store.go for docstrings
// describing the actual API usage.
type Interface interface {
	SeriesPoints(ctx context.Context, opts SeriesPointsOpts) ([]SeriesPoint, error)
}