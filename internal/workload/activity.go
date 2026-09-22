package workload

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

type Input struct {
	RunID                 string    `json:"runId"`
	Mode                  string    `json:"mode"`
	Phase                 string    `json:"phase"`
	Sequence              int64     `json:"sequence"`
	Tenant                string    `json:"tenant"`
	Subtenant             string    `json:"subtenant"`
	FairnessKey           string    `json:"fairnessKey"`
	FairnessWeight        float64   `json:"fairnessWeight"`
	UnclampedWeight       float64   `json:"unclampedWeight"`
	TargetSubtenantShare  float64   `json:"targetSubtenantShare"`
	TargetTenantShare     float64   `json:"targetTenantShare"`
	PredictedCost         float64   `json:"predictedCost"`
	ScheduledAt           time.Time `json:"scheduledAt"`
	BaseWorkDurationNanos int64     `json:"baseWorkDurationNanos"`
	GatewayPod            int       `json:"gatewayPod"`
	RecentCount           int       `json:"recentCount"`
}

type Output struct {
	StartedAt   time.Time `json:"startedAt"`
	CompletedAt time.Time `json:"completedAt"`
	Attempt     int32     `json:"attempt"`
}

func SimulatedModelCall(ctx context.Context, input Input) (Output, error) {
	startedAt := time.Now().UTC()
	duration := time.Duration(float64(input.BaseWorkDurationNanos) * input.PredictedCost)
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return Output{}, fmt.Errorf("simulated model call canceled: %w", ctx.Err())
	case <-timer.C:
	}

	return Output{
		StartedAt:   startedAt,
		CompletedAt: time.Now().UTC(),
		Attempt:     activity.GetInfo(ctx).Attempt,
	}, nil
}
