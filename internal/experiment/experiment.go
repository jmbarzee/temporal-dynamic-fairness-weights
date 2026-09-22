package experiment

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/config"
	"github.com/temporalio/scratch-fairness-weights/internal/results"
	"github.com/temporalio/scratch-fairness-weights/internal/weights"
	"github.com/temporalio/scratch-fairness-weights/internal/workload"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

const ModeHierarchical = "hierarchical"

type pending struct {
	recordIndex int
	handle      client.ActivityHandle
}

type generated struct {
	stream config.Stream
}

func Run(
	ctx context.Context,
	temporalClient client.Client,
	cfg config.Runtime,
	mode string,
	taskQueue string,
	runID string,
) ([]results.Record, error) {
	if mode != ModeHierarchical {
		return nil, fmt.Errorf("unknown mode %q", mode)
	}

	calculators := make([]*weights.Calculator, cfg.GatewayPods)
	for i := range calculators {
		calculator, err := weights.NewCalculator(
			cfg.HistoryWindowDuration,
			cfg.Alpha,
			cfg.Epsilon,
			cfg.WeightScale,
		)
		if err != nil {
			return nil, err
		}
		calculators[i] = calculator
	}

	w := worker.New(temporalClient, taskQueue, worker.Options{
		MaxConcurrentActivityExecutionSize: cfg.WorkerConcurrency,
		MaxConcurrentActivityTaskPollers:   max(1, cfg.WorkerConcurrency),
		WorkerActivitiesPerSecond:          cfg.ExpectedCapacity,
	})
	w.RegisterActivity(workload.SimulatedModelCall)
	workerStarted := false
	startWorker := func() error {
		if workerStarted {
			return nil
		}
		if err := w.Start(); err != nil {
			return fmt.Errorf("start worker: %w", err)
		}
		workerStarted = true
		timer := time.NewTimer(500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return nil
	}
	defer func() {
		if workerStarted {
			w.Stop()
		}
	}()
	if cfg.Warmup == 0 {
		if err := startWorker(); err != nil {
			return nil, err
		}
		if err := primeFairnessQueue(ctx, temporalClient, cfg, taskQueue, runID, mode); err != nil {
			return nil, err
		}
	}

	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed+1))
	var (
		recordsOut []results.Record
		pendingOut []pending
		sequence   int64
	)
	runStarted := time.Now().UTC()
	const tick = 100 * time.Millisecond
	scheduleBatch := func(now time.Time, period string, batch []generated, targetTenantShares map[string]float64) error {
		rng.Shuffle(len(batch), func(i, j int) {
			batch[i], batch[j] = batch[j], batch[i]
		})
		for _, item := range batch {
			stream := item.stream
			pod := rng.IntN(cfg.GatewayPods)
			tenantWeight := cfg.Tenants[stream.Tenant].Weight
			fairnessKey := makeFairnessKey(stream.Tenant, stream.Subtenant)
			decision, err := calculators[pod].ObserveAndCalculate(
				now,
				stream.Tenant,
				stream.Subtenant,
				tenantWeight,
				stream.Cost,
			)
			if err != nil {
				return fmt.Errorf("calculate weight: %w", err)
			}

			sequence++
			input := workload.Input{
				RunID:                 runID,
				Mode:                  mode,
				Phase:                 period,
				Sequence:              sequence,
				Tenant:                stream.Tenant,
				Subtenant:             stream.Subtenant,
				FairnessKey:           fairnessKey,
				FairnessWeight:        decision.Weight,
				UnclampedWeight:       decision.UnclampedWeight,
				TargetSubtenantShare:  decision.Share,
				TargetTenantShare:     targetTenantShares[stream.Tenant],
				PredictedCost:         stream.Cost,
				ScheduledAt:           time.Now().UTC(),
				BaseWorkDurationNanos: int64(time.Duration(cfg.BaseWorkDurationMillis) * time.Millisecond),
				GatewayPod:            pod,
				RecentCount:           decision.RecentCount,
			}
			record := results.Record{Input: input}
			handle, executeErr := temporalClient.ExecuteActivity(ctx, client.StartActivityOptions{
				ID:                     fmt.Sprintf("%s-%s-%d", runID, mode, sequence),
				TaskQueue:              taskQueue,
				ScheduleToCloseTimeout: 30 * time.Minute,
				StartToCloseTimeout:    5 * time.Minute,
				RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
				Priority: temporal.Priority{
					FairnessKey:    fairnessKey,
					FairnessWeight: float32(decision.Weight),
				},
			}, workload.SimulatedModelCall, input)
			record.AcknowledgedAt = time.Now().UTC()
			if executeErr != nil {
				record.Error = executeErr.Error()
			} else {
				pendingOut = append(pendingOut, pending{
					recordIndex: len(recordsOut),
					handle:      handle,
				})
			}
			recordsOut = append(recordsOut, record)
		}
		return nil
	}

	if len(cfg.Streams) > 0 {
		remainders := make([]float64, len(cfg.Streams))
		batchFired := make([][]bool, len(cfg.Streams))
		for i, stream := range cfg.Streams {
			batchFired[i] = make([]bool, len(stream.Profile.Batches))
		}
		ticker := time.NewTicker(tick)
		for elapsed := time.Duration(0); elapsed < cfg.DurationValue; {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return recordsOut, ctx.Err()
			case now := <-ticker.C:
				if !workerStarted && now.Sub(runStarted) >= cfg.Warmup {
					if err := startWorker(); err != nil {
						ticker.Stop()
						return recordsOut, err
					}
				}
				elapsed = now.Sub(runStarted)
				progress := min(1, elapsed.Seconds()/cfg.DurationValue.Seconds())
				rates := make([]float64, len(cfg.Streams))
				var batch []generated
				for i, stream := range cfg.Streams {
					rates[i] = stream.Profile.Rate(progress)
					remainders[i] += rates[i] * tick.Seconds()
					emit := int(remainders[i])
					remainders[i] -= float64(emit)
					for batchIndex, batchProfile := range stream.Profile.Batches {
						if !batchFired[i][batchIndex] && progress >= batchProfile.At {
							batchFired[i][batchIndex] = true
							emit += batchProfile.Tasks
							rates[i] += float64(batchProfile.Tasks) / tick.Seconds()
						}
					}
					for range emit {
						batch = append(batch, generated{stream: stream})
					}
				}
				if err := scheduleBatch(now, dayPeriod(progress), batch, targetTenantSharesForRates(cfg, rates)); err != nil {
					ticker.Stop()
					return recordsOut, err
				}
			}
		}
		ticker.Stop()
	}

	for phaseIndex, phase := range cfg.Phases {
		targetTenantShares := targetTenantShares(cfg, phase)
		remainders := make([]float64, len(phase.Streams))
		phaseStarted := time.Now()
		ticker := time.NewTicker(tick)

		for time.Since(phaseStarted) < cfg.PhaseDurations[phaseIndex] {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return recordsOut, ctx.Err()
			case now := <-ticker.C:
				if !workerStarted && now.Sub(runStarted) >= cfg.Warmup {
					if err := startWorker(); err != nil {
						ticker.Stop()
						return recordsOut, err
					}
				}

				var batch []generated
				for i, stream := range phase.Streams {
					remainders[i] += stream.RequestsPerS * tick.Seconds()
					emit := int(remainders[i])
					remainders[i] -= float64(emit)
					for range emit {
						batch = append(batch, generated{stream: stream})
					}
				}
				if err := scheduleBatch(now, phase.Name, batch, targetTenantShares); err != nil {
					ticker.Stop()
					return recordsOut, err
				}
			}
		}
		ticker.Stop()
	}

	if err := startWorker(); err != nil {
		return recordsOut, err
	}
	resultCtx, cancelResults := context.WithTimeout(ctx, 45*time.Second)
	defer cancelResults()
	var resultWaitGroup sync.WaitGroup
	for _, item := range pendingOut {
		resultWaitGroup.Go(func() {
			var output workload.Output
			if err := item.handle.Get(resultCtx, &output); err != nil {
				recordsOut[item.recordIndex].Error = err.Error()
				return
			}
			recordsOut[item.recordIndex].Output = output
		})
	}
	resultWaitGroup.Wait()
	return recordsOut, nil
}

func targetTenantShares(cfg config.Runtime, phase config.Phase) map[string]float64 {
	active := make(map[string]bool)
	for _, stream := range phase.Streams {
		if stream.RequestsPerS > 0 {
			active[stream.Tenant] = true
		}
	}
	var total float64
	for tenant := range active {
		total += cfg.Tenants[tenant].Weight
	}
	shares := make(map[string]float64, len(active))
	for tenant := range active {
		shares[tenant] = cfg.Tenants[tenant].Weight / total
	}
	return shares
}

func targetTenantSharesForRates(cfg config.Runtime, rates []float64) map[string]float64 {
	active := make(map[string]bool)
	for i, stream := range cfg.Streams {
		if rates[i] >= 0.1 {
			active[stream.Tenant] = true
		}
	}
	var total float64
	for tenant := range active {
		total += cfg.Tenants[tenant].Weight
	}
	shares := make(map[string]float64, len(active))
	for tenant := range active {
		shares[tenant] = cfg.Tenants[tenant].Weight / total
	}
	return shares
}

func dayPeriod(progress float64) string {
	switch {
	case progress < 0.15:
		return "overnight"
	case progress < 0.30:
		return "login"
	case progress < 0.48:
		return "morning"
	case progress < 0.62:
		return "lunch"
	case progress < 0.84:
		return "afternoon"
	default:
		return "evening"
	}
}

func makeFairnessKey(tenant, subtenant string) string {
	key := tenant + "/" + subtenant
	if len(key) <= 64 {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	prefix := tenant
	if len(prefix) > 40 {
		prefix = prefix[:40]
	}
	return fmt.Sprintf("%s/%x", prefix, sum[:8])
}

func primeFairnessQueue(
	ctx context.Context,
	temporalClient client.Client,
	cfg config.Runtime,
	taskQueue string,
	runID string,
	mode string,
) error {
	if err := primeFairnessKey(ctx, temporalClient, cfg, taskQueue, runID, mode, "__warmup__", 0); err != nil {
		return err
	}

	keys := make(map[string]struct{})
	for _, stream := range cfg.Streams {
		key := makeFairnessKey(stream.Tenant, stream.Subtenant)
		keys[key] = struct{}{}
	}

	errs := make(chan error, len(keys))
	var waitGroup sync.WaitGroup
	index := 0
	for key := range keys {
		index++
		keyIndex := index
		fairnessKey := key
		waitGroup.Go(func() {
			errs <- primeFairnessKey(ctx, temporalClient, cfg, taskQueue, runID, mode, fairnessKey, keyIndex)
		})
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}

	settle := time.NewTimer(2 * time.Second)
	defer settle.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-settle.C:
		return nil
	}
}

func primeFairnessKey(
	ctx context.Context,
	temporalClient client.Client,
	cfg config.Runtime,
	taskQueue string,
	runID string,
	mode string,
	key string,
	keyIndex int,
) error {
	for attempt := 1; attempt <= 2; attempt++ {
		input := workload.Input{
			RunID:                 runID,
			Mode:                  mode,
			Phase:                 "warmup",
			Sequence:              -int64(attempt),
			Tenant:                "__warmup__",
			Subtenant:             "__warmup__",
			FairnessKey:           key,
			FairnessWeight:        1,
			UnclampedWeight:       1,
			TargetSubtenantShare:  1,
			TargetTenantShare:     1,
			PredictedCost:         1,
			ScheduledAt:           time.Now().UTC(),
			BaseWorkDurationNanos: int64(time.Duration(cfg.BaseWorkDurationMillis) * time.Millisecond),
		}
		handle, err := temporalClient.ExecuteActivity(ctx, client.StartActivityOptions{
			ID:                     fmt.Sprintf("%s-%s-warmup-%d-%d", runID, mode, keyIndex, attempt),
			TaskQueue:              taskQueue,
			ScheduleToCloseTimeout: 15 * time.Second,
			StartToCloseTimeout:    5 * time.Second,
			RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
			Priority: temporal.Priority{
				FairnessKey:    input.FairnessKey,
				FairnessWeight: 1,
			},
		}, workload.SimulatedModelCall, input)
		if err != nil {
			return fmt.Errorf("schedule fairness warmup: %w", err)
		}
		warmupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		var output workload.Output
		err = handle.Get(warmupCtx, &output)
		cancel()
		if err == nil {
			return nil
		}
		_ = handle.Terminate(ctx, client.TerminateActivityOptions{Reason: "fairness queue warmup timed out"})
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("fairness queue warmup did not dispatch for key %q", key)
}
