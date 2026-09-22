package report

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/config"
	"github.com/temporalio/scratch-fairness-weights/internal/results"
)

var colors = []string{
	"#2563eb", "#dc2626", "#16a34a", "#9333ea", "#ea580c", "#0891b2",
	"#4f46e5", "#be123c", "#65a30d", "#c026d3", "#0d9488", "#ca8a04",
}

type page struct {
	Name      string
	Generated string
	Scenarios []scenarioView
}

type scenarioView struct {
	ID          string
	Name        string
	Description string
	Alpha       float64
	Window      string
	Duration    string
	Modes       []modeView
}

type ScenarioResult struct {
	Config   config.Runtime
	Analyses []results.Analysis
}

type modeView struct {
	Name           string
	Successful     int
	Failed         int
	Duration       string
	MaxBacklog     int
	Contention     string
	MeanShareError string
	Warning        string
	Charts         []chart
}

type chart struct {
	Title    string
	Subtitle string
	YLabel   string
	MaxLabel string
	Lines    []line
	Span     bool
}

type line struct {
	Name         string
	LegendSuffix string
	Color        string
	Segments     []string
	Dashed       bool
}

func Write(path string, cfg config.Runtime, analyses []results.Analysis) error {
	return WriteSuite(path, cfg.Name, []ScenarioResult{{Config: cfg, Analyses: analyses}})
}

func WriteSuite(path string, suiteName string, scenarios []ScenarioResult) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create report: %w", err)
	}
	defer file.Close()

	data := page{
		Name:      suiteName,
		Generated: time.Now().UTC().Format(time.RFC3339),
	}
	for _, scenario := range scenarios {
		view := scenarioView{
			ID:          scenario.Config.Name,
			Name:        scenario.Config.Name,
			Description: scenario.Config.Description,
			Alpha:       scenario.Config.Alpha,
			Window:      scenario.Config.HistoryWindow,
			Duration:    scenario.Config.Duration,
		}
		for _, analysis := range scenario.Analyses {
			view.Modes = append(view.Modes, buildMode(scenario.Config, analysis))
		}
		data.Scenarios = append(data.Scenarios, view)
	}
	if err := reportTemplate.Execute(file, data); err != nil {
		return fmt.Errorf("render report: %w", err)
	}
	return nil
}

func WriteJSON(path string, cfg config.Runtime, analyses []results.Analysis) error {
	return WriteSuiteJSON(path, cfg.Name, []ScenarioResult{{Config: cfg, Analyses: analyses}})
}

func WriteSuiteJSON(path string, suiteName string, scenarios []ScenarioResult) error {
	payload := struct {
		Name      string `json:"name"`
		Scenarios []struct {
			Scenario config.Config      `json:"scenario"`
			Analyses []results.Analysis `json:"analyses"`
		} `json:"scenarios"`
	}{
		Name: suiteName,
	}
	for _, scenario := range scenarios {
		payload.Scenarios = append(payload.Scenarios, struct {
			Scenario config.Config      `json:"scenario"`
			Analyses []results.Analysis `json:"analyses"`
		}{Scenario: scenario.Config.Config, Analyses: scenario.Analyses})
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode analysis: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write analysis: %w", err)
	}
	return nil
}

func buildMode(cfg config.Runtime, analysis results.Analysis) modeView {
	tenants := sortedTenantNames(cfg)
	view := modeView{
		Name:       analysis.Mode,
		Successful: analysis.Successful,
		Failed:     analysis.Failed,
		Duration:   analysis.FinishedAt.Sub(analysis.StartedAt).Round(time.Millisecond).String(),
	}
	for _, bucket := range analysis.Buckets {
		view.MaxBacklog = max(view.MaxBacklog, bucket.OutstandingApprox)
	}
	contentionFraction := contentionFraction(cfg, analysis)
	view.Contention = fmt.Sprintf("%.0f%%", contentionFraction*100)
	shareError := contendedShareError(cfg, analysis)
	view.MeanShareError = fmt.Sprintf("%.1f%%", shareError*100)
	if analysis.Failed > 0 {
		view.Warning = fmt.Sprintf("%d activities failed; inspect the JSONL output", analysis.Failed)
	} else if contentionFraction < 0.20 || contentionFraction > 0.60 {
		view.Warning = fmt.Sprintf("Observed contention was %.0f%%; this run is outside the intended 20–60%% staging range.", contentionFraction*100)
	}

	if analysis.Mode == "hierarchical" {
		view.Charts = append(view.Charts,
			mapChart("Backlog (approx.)", "Outstanding schedules minus starts, summed by tenant", "tasks", analysis.Buckets, tenants, cfg.Bucket, func(b results.Bucket, tenant string) float64 {
				return float64(b.OutstandingByTenant[tenant])
			}, false),
			mapChart("Backlog (approx.)", "Full decomposition of the tenant chart at left", "tasks", analysis.Buckets, sortedSubtenantNames(analysis), cfg.Bucket, func(b results.Bucket, subtenant string) float64 {
				return float64(b.OutstandingBySubtenant[subtenant])
			}, false),
		)
	}

	view.Charts = append(view.Charts, queuedChart(cfg, analysis, tenants, false))
	if analysis.Mode == "hierarchical" {
		subtenants := sortedSubtenantNames(analysis)
		view.Charts = append(view.Charts, queuedChart(cfg, analysis, subtenants, true))
	}

	view.Charts = append(view.Charts, mapChart(
		"Dispatched",
		"Activities started per second, summed by tenant",
		"starts/s",
		analysis.Buckets,
		tenants,
		cfg.Bucket,
		func(b results.Bucket, tenant string) float64 {
			return float64(b.StartedByTenant[tenant]) / cfg.Bucket.Seconds()
		},
		false,
	))
	if analysis.Mode == "hierarchical" {
		subtenants := sortedSubtenantNames(analysis)
		view.Charts = append(view.Charts, mapChart(
			"Dispatched",
			"Full decomposition of the tenant chart at left",
			"starts/s",
			analysis.Buckets,
			subtenants,
			cfg.Bucket,
			func(b results.Bucket, subtenant string) float64 {
				return float64(b.StartedBySubtenant[subtenant]) / cfg.Bucket.Seconds()
			},
			false,
		))
	}

	view.Charts = append(view.Charts, shareChart(
		"% Capacity (actual)",
		"Observed share of Activity starts",
		analysis.Buckets,
		tenants,
	))
	if analysis.Mode == "hierarchical" {
		view.Charts = append(view.Charts, subtenantShareChart(
			"% Capacity (actual)",
			"Full decomposition of the tenant chart at left",
			analysis.Buckets,
			sortedSubtenantNames(analysis),
		))
	}

	view.Charts = append(view.Charts, tenantTargetChart(analysis.Buckets, tenants))
	if analysis.Mode == "hierarchical" {
		view.Charts = append(view.Charts, subtenantTargetChart(analysis.Buckets, sortedSubtenantNames(analysis)))
	}

	if weightChart := buildWeightChart(analysis); len(weightChart.Lines) > 0 {
		weightChart.Span = true
		view.Charts = append(view.Charts, weightChart)
	}
	return view
}

func queuedChart(cfg config.Runtime, analysis results.Analysis, names []string, subtenant bool) chart {
	actual := make(map[string][]float64, len(names))
	intended := make(map[string][]float64, len(names))
	var maximum float64
	var absoluteError float64
	var samples int

	for bucketIndex, bucket := range analysis.Buckets {
		startProgress := float64(bucketIndex) * cfg.Bucket.Seconds() / cfg.DurationValue.Seconds()
		endProgress := float64(bucketIndex+1) * cfg.Bucket.Seconds() / cfg.DurationValue.Seconds()
		midProgress := (startProgress + endProgress) / 2
		intendedAtBucket := make(map[string]float64, len(names))
		for _, stream := range cfg.Streams {
			name := stream.Tenant
			if subtenant {
				name = stream.Tenant + "/" + stream.Subtenant
			}
			intendedAtBucket[name] += stream.Profile.Rate(midProgress)
			for _, batch := range stream.Profile.Batches {
				if batch.At >= startProgress && batch.At < endProgress {
					intendedAtBucket[name] += float64(batch.Tasks) / cfg.Bucket.Seconds()
				}
			}
		}
		for _, name := range names {
			var observed float64
			if subtenant {
				observed = float64(bucket.ScheduledBySubtenant[name]) / cfg.Bucket.Seconds()
			} else {
				observed = float64(bucket.ScheduledByTenant[name]) / cfg.Bucket.Seconds()
			}
			expected := intendedAtBucket[name]
			actual[name] = append(actual[name], observed)
			intended[name] = append(intended[name], expected)
			maximum = max(maximum, observed, expected)
			absoluteError += math.Abs(observed - expected)
			samples++
		}
	}

	scope := "summed by tenant"
	if subtenant {
		scope = "by sub-tenant"
	}
	chart := makeChart(
		"Queued",
		fmt.Sprintf("Solid: observed; dashed: configured curve, %s · MAE %.1f tasks/s", scope, absoluteError/float64(max(1, samples))),
		"tasks/s",
		names,
		actual,
		maximum,
		false,
	)
	palette := seriesColors(names)
	for i, name := range names {
		chart.Lines = append(chart.Lines, line{
			Name:         name,
			LegendSuffix: " curve",
			Color:        palette[i],
			Segments:     segments(intended[name], maximum),
			Dashed:       true,
		})
	}
	return chart
}

func mapChart(
	title, subtitle, yLabel string,
	buckets []results.Bucket,
	names []string,
	bucketSize time.Duration,
	value func(results.Bucket, string) float64,
	dashed bool,
) chart {
	_ = bucketSize
	var maximum float64
	values := make(map[string][]float64, len(names))
	for _, name := range names {
		for _, bucket := range buckets {
			v := value(bucket, name)
			values[name] = append(values[name], v)
			maximum = max(maximum, v)
		}
	}
	return makeChart(title, subtitle, yLabel, names, values, maximum, dashed)
}

func shareChart(title, subtitle string, buckets []results.Bucket, tenants []string) chart {
	values := make(map[string][]float64)
	for _, bucket := range buckets {
		var total float64
		for _, tenant := range tenants {
			total += float64(bucket.StartedByTenant[tenant])
		}
		for _, tenant := range tenants {
			amount := float64(bucket.StartedByTenant[tenant])
			if total > 0 {
				values[tenant] = append(values[tenant], amount/total)
			} else {
				values[tenant] = append(values[tenant], math.NaN())
			}
		}
	}
	result := makeChart(title, subtitle, "share", tenants, values, 1, false)
	result.MaxLabel = "100%"
	return result
}

func subtenantShareChart(title, subtitle string, buckets []results.Bucket, subtenants []string) chart {
	values := make(map[string][]float64)
	for _, bucket := range buckets {
		var total float64
		for _, subtenant := range subtenants {
			total += float64(bucket.StartedBySubtenant[subtenant])
		}
		for _, subtenant := range subtenants {
			amount := float64(bucket.StartedBySubtenant[subtenant])
			if total > 0 {
				values[subtenant] = append(values[subtenant], amount/total)
			} else {
				values[subtenant] = append(values[subtenant], math.NaN())
			}
		}
	}
	result := makeChart(title, subtitle, "share", subtenants, values, 1, false)
	result.MaxLabel = "100%"
	return result
}

func tenantTargetChart(buckets []results.Bucket, tenants []string) chart {
	values := make(map[string][]float64, len(tenants))
	last := make(map[string]float64, len(tenants))
	for _, bucket := range buckets {
		for _, tenant := range tenants {
			if target, ok := bucket.TargetShareByTenant[tenant]; ok {
				last[tenant] = target
			}
			if bucket.StartedByTenant[tenant] == 0 {
				values[tenant] = append(values[tenant], math.NaN())
			} else {
				values[tenant] = append(values[tenant], last[tenant])
			}
		}
	}
	result := makeChart(
		"% Capacity (target)",
		"Schedule-time target derived from configured tenant entitlements",
		"share",
		tenants,
		values,
		1,
		true,
	)
	result.MaxLabel = "100%"
	return result
}

func subtenantTargetChart(buckets []results.Bucket, subtenants []string) chart {
	values := make(map[string][]float64, len(subtenants))
	last := make(map[string]float64, len(subtenants))
	for _, bucket := range buckets {
		for _, subtenant := range subtenants {
			if target, ok := bucket.TargetShareBySubtenant[subtenant]; ok {
				last[subtenant] = target
			}
			if bucket.StartedBySubtenant[subtenant] == 0 {
				values[subtenant] = append(values[subtenant], math.NaN())
			} else {
				values[subtenant] = append(values[subtenant], last[subtenant])
			}
		}
	}
	result := makeChart(
		"% Capacity (target)",
		"Full schedule-time target decomposition of the tenant chart at left",
		"share",
		subtenants,
		values,
		1,
		true,
	)
	result.MaxLabel = "100%"
	return result
}

func buildWeightChart(analysis results.Analysis) chart {
	counts := make(map[string]int)
	for _, record := range analysis.Records {
		counts[record.Input.FairnessKey]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		return counts[b] - counts[a]
	})
	if len(keys) > 8 {
		keys = keys[:8]
	}
	values := make(map[string][]float64, len(keys))
	var maximum float64
	for _, key := range keys {
		for _, bucket := range analysis.Buckets {
			v, ok := bucket.AverageWeightByKey[key]
			if !ok {
				v = math.NaN()
			}
			values[key] = append(values[key], v)
			if !math.IsNaN(v) {
				maximum = max(maximum, v)
			}
		}
	}
	return makeChart("Fairness weight", "Average stamped weight for the eight busiest keys", "weight", keys, values, maximum, false)
}

func makeChart(title, subtitle, yLabel string, names []string, values map[string][]float64, maximum float64, dashed bool) chart {
	maximum = max(maximum, 1)
	result := chart{
		Title:    title,
		Subtitle: subtitle,
		YLabel:   yLabel,
		MaxLabel: fmt.Sprintf("%.1f", maximum),
	}
	legendSuffix := ""
	if dashed {
		legendSuffix = " target"
	}
	palette := seriesColors(names)
	for i, name := range names {
		result.Lines = append(result.Lines, line{
			Name:         name,
			LegendSuffix: legendSuffix,
			Color:        palette[i],
			Segments:     segments(values[name], maximum),
			Dashed:       dashed,
		})
	}
	return result
}

func seriesColors(names []string) []string {
	palette := make([]string, len(names))
	hierarchical := false
	tenantSet := make(map[string]struct{})
	byTenant := make(map[string][]int)
	for index, name := range names {
		tenant, _, found := strings.Cut(name, "/")
		if !found {
			continue
		}
		hierarchical = true
		tenantSet[tenant] = struct{}{}
		byTenant[tenant] = append(byTenant[tenant], index)
	}
	if !hierarchical {
		for index := range names {
			palette[index] = colors[index%len(colors)]
		}
		return palette
	}

	tenants := make([]string, 0, len(tenantSet))
	for tenant := range tenantSet {
		tenants = append(tenants, tenant)
	}
	slices.Sort(tenants)
	for tenantIndex, tenant := range tenants {
		indexes := byTenant[tenant]
		for variant, seriesIndex := range indexes {
			factor := 0.0
			if len(indexes) > 1 {
				factor = 0.55 * float64(variant) / float64(len(indexes)-1)
			}
			palette[seriesIndex] = mixWithWhite(colors[tenantIndex%len(colors)], factor)
		}
	}
	return palette
}

func mixWithWhite(color string, factor float64) string {
	if len(color) != 7 || color[0] != '#' {
		return color
	}
	value, err := strconv.ParseUint(color[1:], 16, 24)
	if err != nil {
		return color
	}
	red := float64((value >> 16) & 0xff)
	green := float64((value >> 8) & 0xff)
	blue := float64(value & 0xff)
	mix := func(component float64) uint64 {
		return uint64(math.Round(component + (255-component)*factor))
	}
	return fmt.Sprintf("#%02x%02x%02x", mix(red), mix(green), mix(blue))
}

func segments(values []float64, maximum float64) []string {
	if len(values) == 0 {
		return nil
	}
	const (
		left   = 52.0
		top    = 12.0
		width  = 816.0
		height = 184.0
	)
	var result []string
	parts := make([]string, 0, len(values))
	flush := func() {
		if len(parts) >= 2 {
			result = append(result, strings.Join(parts, " "))
		}
		parts = parts[:0]
	}
	denominator := max(1, len(values)-1)
	for i, value := range values {
		if math.IsNaN(value) {
			flush()
			continue
		}
		x := left + float64(i)*width/float64(denominator)
		y := top + height*(1-min(1, max(0, value/maximum)))
		parts = append(parts, fmt.Sprintf("%.1f,%.1f", x, y))
	}
	flush()
	return result
}

func sortedTenantNames(cfg config.Runtime) []string {
	names := make([]string, 0, len(cfg.Tenants))
	for tenant := range cfg.Tenants {
		names = append(names, tenant)
	}
	slices.Sort(names)
	return names
}

func sortedSubtenantNames(analysis results.Analysis) []string {
	seen := make(map[string]struct{})
	for _, record := range analysis.Records {
		seen[record.Input.Tenant+"/"+record.Input.Subtenant] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func contentionFraction(cfg config.Runtime, analysis results.Analysis) float64 {
	if len(analysis.Buckets) == 0 {
		return 0
	}
	threshold := max(5, cfg.WorkerConcurrency/2)
	var contended int
	for _, bucket := range analysis.Buckets {
		if bucket.OutstandingApprox >= threshold {
			contended++
		}
	}
	return float64(contended) / float64(len(analysis.Buckets))
}

func contendedShareError(cfg config.Runtime, analysis results.Analysis) float64 {
	tenants := sortedTenantNames(cfg)
	targets := make(map[string]float64)
	var sum float64
	var count int
	for _, bucket := range analysis.Buckets {
		for tenant, target := range bucket.TargetShareByTenant {
			targets[tenant] = target
		}
		allTenantsBacklogged := true
		var total float64
		for _, tenant := range tenants {
			if targets[tenant] > 0 && bucket.OutstandingByTenant[tenant] < 5 {
				allTenantsBacklogged = false
			}
			total += bucket.ServiceByTenant[tenant]
		}
		if !allTenantsBacklogged || total < 5 {
			continue
		}
		for _, tenant := range tenants {
			target, ok := targets[tenant]
			if !ok {
				continue
			}
			actual := bucket.ServiceByTenant[tenant] / total
			sum += math.Abs(actual - target)
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

var reportTemplate = template.Must(template.New("report").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dynamic Fairness Validation</title>
<style>
:root { color-scheme: light dark; --bg:#f8fafc; --surface:#fff; --text:#0f172a; --muted:#64748b; --line:#cbd5e1; --warn:#b45309; }
@media (prefers-color-scheme:dark) { :root { --bg:#0f172a; --surface:#172033; --text:#e2e8f0; --muted:#94a3b8; --line:#334155; --warn:#fbbf24; } }
* { box-sizing:border-box; } body { margin:0; background:var(--bg); color:var(--text); font:14px/1.5 system-ui,sans-serif; }
main { width:min(1800px,100%); margin:auto; padding:clamp(12px,2vw,32px); } h1 { font-size:26px; margin:0; } h2 { margin:36px 0 4px; } h3.mode-title { margin:24px 0 8px; }
.meta,.subtitle { color:var(--muted); } .stats { display:flex; gap:24px; flex-wrap:wrap; margin:16px 0; }
.stat strong { display:block; font-size:20px; }.warning { color:var(--warn); font-weight:600; }
.scenario { border-top:1px solid var(--line); margin-top:28px; }.scenario:first-of-type { border-top:0; }.grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; }
.column-heading { color:var(--muted); font-size:12px; font-weight:700; letter-spacing:.08em; text-transform:uppercase; padding:0 4px; }.span-two { grid-column:1 / -1; }
.chart { background:var(--surface); border:1px solid var(--line); border-radius:8px; padding:14px; }
.chart h4 { margin:0; font-size:15px; }.chart p { margin:2px 0 8px; color:var(--muted); font-size:12px; }
svg { width:100%; height:auto; }.axis { stroke:var(--line); stroke-width:1; }.label { fill:var(--muted); font-size:11px; }
.series { fill:none; stroke-width:2; vector-effect:non-scaling-stroke; }.legend { display:flex; gap:12px; flex-wrap:wrap; font-size:11px; color:var(--muted); }
.swatch { display:inline-block; width:14px; height:2px; margin-right:4px; vertical-align:middle; }
.nav { display:flex; flex-wrap:wrap; gap:8px; margin:16px 0 0; }.nav button { appearance:none; background:var(--surface); color:var(--text); border:1px solid var(--line); border-radius:999px; padding:6px 12px; font:inherit; cursor:pointer; }.nav button[aria-pressed="true"] { background:var(--text); color:var(--bg); border-color:var(--text); }.scenario[hidden] { display:none; }
@media (max-width:1000px) { .grid { grid-template-columns:minmax(0,1fr); } .column-heading { display:none; } .span-two { grid-column:auto; } }
</style>
</head>
<body><main>
<h1>Dynamic Fairness Validation</h1>
<div class="meta">Suite {{.Name}} · generated {{.Generated}}</div>
<nav class="nav" aria-label="Test views">{{range $index, $scenario := .Scenarios}}<button type="button" data-target="{{$scenario.ID}}" aria-pressed="{{if eq $index 0}}true{{else}}false{{end}}">{{$scenario.Name}}</button>{{end}}</nav>
{{range $index, $scenario := .Scenarios}}
<section class="scenario" id="{{$scenario.ID}}"{{if ne $index 0}} hidden{{end}}>
<h2>{{.Name}}</h2>
<div class="meta">duration {{.Duration}} · α={{printf "%.2f" .Alpha}} · history window {{.Window}}</div>
{{if .Description}}<p>{{.Description}}</p>{{end}}
{{range .Modes}}
<div class="mode">
<h3 class="mode-title">{{.Name}}</h3>
<div class="stats">
  <div class="stat"><strong>{{.Successful}}</strong>completed</div>
  <div class="stat"><strong>{{.Failed}}</strong>failed</div>
  <div class="stat"><strong>{{.Duration}}</strong>duration</div>
  <div class="stat"><strong>{{.MaxBacklog}}</strong>max backlog proxy</div>
  <div class="stat"><strong>{{.Contention}}</strong>time contended</div>
  <div class="stat"><strong>{{.MeanShareError}}</strong>contended tenant-share error</div>
</div>
{{if .Warning}}<p class="warning">{{.Warning}}</p>{{end}}
<div class="grid">
<div class="column-heading">Tenant</div>
<div class="column-heading">Sub-tenant</div>
{{range .Charts}}
<article class="chart{{if .Span}} span-two{{end}}">
<h4>{{.Title}}</h4><p>{{.Subtitle}}</p>
<svg viewBox="0 0 900 220" role="img" aria-label="{{.Title}}">
  <line class="axis" x1="52" y1="12" x2="52" y2="196"></line>
  <line class="axis" x1="52" y1="196" x2="868" y2="196"></line>
  <text class="label" x="4" y="18">{{.MaxLabel}}</text>
  <text class="label" x="24" y="200">0</text>
  <text class="label" x="450" y="216">time</text>
  {{range .Lines}}{{$line := .}}{{range .Segments}}<polyline class="series" points="{{.}}" stroke="{{$line.Color}}" {{if $line.Dashed}}stroke-dasharray="6 5"{{end}}></polyline>{{end}}{{end}}
</svg>
<div class="legend">{{range .Lines}}<span><i class="swatch" style="background:{{.Color}}"></i>{{.Name}}{{.LegendSuffix}}</span>{{end}}</div>
<div class="subtitle">Y-axis: {{.YLabel}}</div>
</article>
{{end}}
</div>
</div>
{{end}}
</section>
{{end}}
</main>
<script>
document.addEventListener("click", function (event) {
  const button = event.target.closest("button[data-target]");
  if (!button) return;
  const target = button.getAttribute("data-target");
  document.querySelectorAll(".scenario").forEach(function (section) {
    section.hidden = section.id !== target;
  });
  document.querySelectorAll(".nav button[data-target]").forEach(function (candidate) {
    candidate.setAttribute("aria-pressed", candidate === button ? "true" : "false");
  });
  window.scrollTo({ top: 0, behavior: "smooth" });
});
</script>
</body></html>`))
