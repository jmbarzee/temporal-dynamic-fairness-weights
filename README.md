# Dynamic fairness-weight validation

This experiment tests whether schedule-time Activity weights derived from representative local traffic history approximate hierarchical tenant/sub-tenant fairness. The fairness key is `tenant/subtenant`, and the weight is:

   ```text
   score = (recentCount + epsilon)^alpha
   subtenantShare = score / sum(sibling scores)
   weight = clamp(scale * tenantWeight * subtenantShare / predictedCost, 0.001, 1000)
   ```

Each simulated gateway pod has independent recent-history state. Requests are randomly assigned to pods, approximating representative but incomplete local observations.

## Requirements

- Go 1.25.4 or newer.
- Temporal CLI 1.9 or newer with bundled Server 1.31 or newer.
- Standalone Activities and Task Queue Fairness are Public Preview features.

The stock Temporal Grafana dashboards do not expose fairness-key metrics. This program records every schedule and Activity start itself and generates a self-contained HTML report.

## Start a development server

```bash
temporal server start-dev \
  --dynamic-config-value matching.enableFairness=true \
  --dynamic-config-value matching.autoEnableV2=true
```

Standalone Activities are enabled by default on current development servers.

## Run the experiment

In another terminal:

```bash
go run ./cmd/fairness-lab
```

Connection settings can be supplied through flags or environment variables:

```bash
TEMPORAL_ADDRESS=localhost:7233 \
TEMPORAL_NAMESPACE=default \
go run ./cmd/fairness-lab \
  -scenario scenarios/default.json \
  -out out
```

The default suite runs five named hierarchical load-profile scenarios. Run one scenario instead with:

```bash
go run ./cmd/fairness-lab \
  -scenario scenarios/hot-key-saw-handoff.json \
  -out out
```

## Outputs

- `out/report.html`: self-contained graphs and summary statistics.
- `out/<scenario>/hierarchical.jsonl`: one record per composite-key Activity.
- `out/analysis.json`: scenario configuration and aggregated time buckets.

Open the report:

```bash
open out/report.html
```

After changing only the report code, regenerate from the existing JSONL without rerunning Activities:

```bash
go run ./cmd/fairness-lab -out out -report-only
```

The report includes navigation across all scenarios and responsive, paired tenant and sub-tenant views of:

- offered volume and dispatch throughput;
- actual versus target dispatch share;
- predicted-cost-weighted service share;
- offered predicted work versus fixed Worker capacity;
- schedule-to-start p95;
- an observed backlog proxy;
- dynamically assigned weights.

Interpret only intervals with a sustained backlog. When there is no backlog, Temporal dispatches work immediately and fairness weights have no visible effect.

## Scenario controls

Edit the named files listed in `scenarios/suite.json` to change:

- `alpha`: `0` gives equal observed siblings, values between `0` and `1` favor volume sublinearly, and `1` is proportional to recent volume;
- `historyWindow`: how quickly inactive siblings disappear from a pod's estimate;
- `gatewayPods`: number of independent local estimators;
- tenant business weights;
- `wake`: the smooth workday rise and fall for each sub-tenant;
- positive or negative Gaussian `bumps` for login, lunch, and afternoon effects;
- sinusoidal `rhythms` for interval-triggered traffic;
- all-at-once `batches`, large `saws`, smooth `plateaus`, and `triangles`;
- per-stream rate caps and predicted costs;
- Worker concurrency and simulated work duration.

Use a fresh Task Queue for each run. The executable does this automatically so previous fairness pass state does not contaminate the result.

## Development checks

```bash
gofmt -w cmd internal
go test ./...
go vet ./...
```
