package amqp

import (
	"context"
	"fmt"
	"time"

	"go.k6.io/k6/lib"
	"go.k6.io/k6/stats"
)

const statsPrefix = "k6_amqp"

var (
	RPCPublishCounter = stats.New(fmt.Sprintf("%s_rpc_published_total", statsPrefix), stats.Counter)
	RPCConsumeCounter = stats.New(fmt.Sprintf("%s_rpc_consumed_total", statsPrefix), stats.Counter)
	RPCEncoding       = stats.New(fmt.Sprintf("%s_rpc_encoding_duration", statsPrefix), stats.Trend)
	RPCDecoding       = stats.New(fmt.Sprintf("%s_rpc_decoding_duration", statsPrefix), stats.Trend)
)

func IncrementCounter(ctx context.Context, counter *stats.Metric) {
	state := lib.GetState(ctx)
	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   time.Now(),
		Metric: counter,
		Value:  1,
	})
}

func IncrementCounterWithTags(ctx context.Context, counter *stats.Metric, tags map[string]string) {
	state := lib.GetState(ctx)
	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   time.Now(),
		Metric: counter,
		Tags:   stats.IntoSampleTags(&tags),
		Value:  1,
	})
}

func ObserveTrend(ctx context.Context, trend *stats.Metric, dur float64) {
	state := lib.GetState(ctx)

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   time.Now(),
		Metric: trend,
		Value:  dur,
	})
}

func ObserveTrendWithTags(ctx context.Context, trend *stats.Metric, dur float64, tags map[string]string) {
	state := lib.GetState(ctx)

	stats.PushIfNotDone(ctx, state.Samples, stats.Sample{
		Time:   time.Now(),
		Metric: trend,
		Value:  dur,
		Tags:   stats.IntoSampleTags(&tags),
	})
}
