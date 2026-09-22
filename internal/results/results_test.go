package results

import (
	"testing"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/workload"
)

func TestAnalyzeBucketsEventsAndLatency(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []Record{
		{
			Input: workload.Input{
				Tenant:            "a",
				Subtenant:         "one",
				FairnessKey:       "a/one",
				FairnessWeight:    2,
				UnclampedWeight:   2,
				TargetTenantShare: 0.5,
				PredictedCost:     2,
				ScheduledAt:       start,
			},
			Output: workload.Output{
				StartedAt:   start.Add(150 * time.Millisecond),
				CompletedAt: start.Add(250 * time.Millisecond),
			},
		},
		{
			Input: workload.Input{
				Tenant:            "b",
				Subtenant:         "two",
				FairnessKey:       "b/two",
				FairnessWeight:    MinTestWeight,
				UnclampedWeight:   MinTestWeight / 10,
				TargetTenantShare: 0.5,
				PredictedCost:     1,
				ScheduledAt:       start.Add(600 * time.Millisecond),
			},
			Output: workload.Output{
				StartedAt:   start.Add(1200 * time.Millisecond),
				CompletedAt: start.Add(1300 * time.Millisecond),
			},
		},
	}

	analysis := Analyze("test", records, time.Second)
	if analysis.Successful != 2 || analysis.Failed != 0 {
		t.Fatalf("unexpected counts: %+v", analysis)
	}
	if len(analysis.Buckets) != 2 {
		t.Fatalf("expected two buckets, got %d", len(analysis.Buckets))
	}
	first := analysis.Buckets[0]
	if first.ScheduledByTenant["a"] != 1 || first.ScheduledByTenant["b"] != 1 {
		t.Fatalf("unexpected schedules: %+v", first.ScheduledByTenant)
	}
	if first.StartedByTenant["a"] != 1 || first.ServiceByTenant["a"] != 2 {
		t.Fatalf("unexpected starts: %+v", first)
	}
	if first.ScheduleToStartP95MS != 150 {
		t.Fatalf("expected 150ms p95, got %v", first.ScheduleToStartP95MS)
	}
	if first.ClampedWeights != 1 {
		t.Fatalf("expected one clamped weight, got %d", first.ClampedWeights)
	}
}

const MinTestWeight = 0.001
