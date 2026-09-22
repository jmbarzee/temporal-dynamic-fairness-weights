package report

import (
	"bytes"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/config"
	"github.com/temporalio/scratch-fairness-weights/internal/results"
)

func TestSegmentsDoNotBridgeMissingRanges(t *testing.T) {
	got := segments([]float64{0.5, 0.5, math.NaN(), math.NaN(), 0.5, 0.5}, 1)
	if len(got) != 2 {
		t.Fatalf("expected two line segments, got %d: %v", len(got), got)
	}
}

func TestSubtenantColorsAreTenantColorVariants(t *testing.T) {
	palette := seriesColors([]string{"a/one", "a/two", "b/one"})
	if palette[0] != colors[0] {
		t.Fatalf("expected first tenant to use its base color, got %s", palette[0])
	}
	if palette[1] == palette[0] {
		t.Fatalf("expected sibling sub-tenants to use distinct variants")
	}
	if palette[2] != colors[1] {
		t.Fatalf("expected second tenant to use the second base color, got %s", palette[2])
	}
}

func TestQueuedChartIncludesConfiguredCurve(t *testing.T) {
	profile := &config.LoadProfile{
		BaseRate: 5,
		Batches:  []config.Batch{{At: 0.5, Tasks: 10}},
	}
	cfg := config.Runtime{
		Config: config.Config{
			Duration: "10s",
			Streams: []config.Stream{{
				Tenant:    "a",
				Subtenant: "one",
				Cost:      1,
				Profile:   profile,
			}},
		},
		DurationValue: 10 * time.Second,
		Bucket:        time.Second,
	}
	analysis := results.Analysis{Buckets: make([]results.Bucket, 10)}
	for i := range analysis.Buckets {
		count := 5
		if i == 5 {
			count += 10
		}
		analysis.Buckets[i].ScheduledByTenant = map[string]int{"a": count}
	}

	chart := queuedChart(cfg, analysis, []string{"a"}, false)
	if len(chart.Lines) != 2 {
		t.Fatalf("expected observed and configured lines, got %d", len(chart.Lines))
	}
	if chart.Lines[1].LegendSuffix != " curve" || !chart.Lines[1].Dashed {
		t.Fatalf("expected configured curve to be dashed, got %+v", chart.Lines[1])
	}
	if !strings.Contains(chart.Subtitle, "MAE 0.0 tasks/s") {
		t.Fatalf("expected exact curve match, got %q", chart.Subtitle)
	}
}

func TestReportShowsOneScenarioWithViewButtons(t *testing.T) {
	var rendered bytes.Buffer
	err := reportTemplate.Execute(&rendered, page{
		Name: "suite",
		Scenarios: []scenarioView{
			{ID: "first", Name: "first"},
			{ID: "second", Name: "second"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	html := rendered.String()
	if strings.Count(html, `button type="button" data-target=`) != 2 {
		t.Fatalf("expected two view buttons")
	}
	if !strings.Contains(html, `<section class="scenario" id="first">`) {
		t.Fatalf("expected first scenario to be visible")
	}
	if !strings.Contains(html, `<section class="scenario" id="second" hidden>`) {
		t.Fatalf("expected second scenario to be hidden")
	}
}

func TestTenantTargetsHiddenWithoutActualDispatch(t *testing.T) {
	buckets := []results.Bucket{
		{StartedByTenant: map[string]int{"a": 1}, TargetShareByTenant: map[string]float64{"a": 0.5}},
		{StartedByTenant: map[string]int{"a": 1}, TargetShareByTenant: map[string]float64{"a": 0.5}},
		{StartedByTenant: map[string]int{}, TargetShareByTenant: map[string]float64{"a": 0.5}},
		{StartedByTenant: map[string]int{}, TargetShareByTenant: map[string]float64{"a": 0.5}},
		{StartedByTenant: map[string]int{"a": 1}, TargetShareByTenant: map[string]float64{"a": 0.5}},
		{StartedByTenant: map[string]int{"a": 1}, TargetShareByTenant: map[string]float64{"a": 0.5}},
	}
	target := tenantTargetChart(buckets, []string{"a"})
	if len(target.Lines) != 1 || len(target.Lines[0].Segments) != 2 {
		t.Fatalf("expected target to contain two visible ranges, got %+v", target.Lines)
	}
}
