package results

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/workload"
)

type Record struct {
	Input          workload.Input  `json:"input"`
	Output         workload.Output `json:"output"`
	AcknowledgedAt time.Time       `json:"acknowledgedAt"`
	Error          string          `json:"error,omitempty"`
}

type Bucket struct {
	Index                    int                `json:"index"`
	Start                    time.Time          `json:"start"`
	ScheduledByTenant        map[string]int     `json:"scheduledByTenant"`
	ScheduledBySubtenant     map[string]int     `json:"scheduledBySubtenant"`
	ScheduledServiceByTenant map[string]float64 `json:"scheduledServiceByTenant"`
	StartedByTenant          map[string]int     `json:"startedByTenant"`
	StartedBySubtenant       map[string]int     `json:"startedBySubtenant"`
	ServiceByTenant          map[string]float64 `json:"serviceByTenant"`
	ServiceBySubtenant       map[string]float64 `json:"serviceBySubtenant"`
	TargetShareByTenant      map[string]float64 `json:"targetShareByTenant"`
	TargetShareBySubtenant   map[string]float64 `json:"targetShareBySubtenant"`
	AverageWeightByKey       map[string]float64 `json:"averageWeightByKey"`
	ScheduleToStartP50MS     float64            `json:"scheduleToStartP50Ms"`
	ScheduleToStartP95MS     float64            `json:"scheduleToStartP95Ms"`
	OutstandingApprox        int                `json:"outstandingApprox"`
	OutstandingByTenant      map[string]int     `json:"outstandingByTenant"`
	OutstandingBySubtenant   map[string]int     `json:"outstandingBySubtenant"`
	ClampedWeights           int                `json:"clampedWeights"`
}

type Analysis struct {
	Mode       string        `json:"mode"`
	StartedAt  time.Time     `json:"startedAt"`
	FinishedAt time.Time     `json:"finishedAt"`
	BucketSize time.Duration `json:"bucketSize"`
	Records    []Record      `json:"-"`
	Buckets    []Bucket      `json:"buckets"`
	Successful int           `json:"successful"`
	Failed     int           `json:"failed"`
}

type bucketWork struct {
	Bucket
	latencies            []float64
	weightSums           map[string]float64
	weightCount          map[string]int
	targetSums           map[string]float64
	targetCount          map[string]int
	targetSubtenantSums  map[string]float64
	targetSubtenantCount map[string]int
}

func Analyze(mode string, records []Record, bucketSize time.Duration) Analysis {
	analysis := Analysis{Mode: mode, BucketSize: bucketSize, Records: records}
	if len(records) == 0 {
		return analysis
	}

	start := records[0].Input.ScheduledAt
	end := start
	for _, record := range records {
		if record.Input.ScheduledAt.Before(start) {
			start = record.Input.ScheduledAt
		}
		if record.Input.ScheduledAt.After(end) {
			end = record.Input.ScheduledAt
		}
		if record.Output.CompletedAt.After(end) {
			end = record.Output.CompletedAt
		}
		if record.Error == "" {
			analysis.Successful++
		} else {
			analysis.Failed++
		}
	}
	analysis.StartedAt = start
	analysis.FinishedAt = end
	count := max(1, int(end.Sub(start)/bucketSize)+1)
	work := make([]bucketWork, count)
	for i := range work {
		work[i] = newBucketWork(i, start.Add(time.Duration(i)*bucketSize))
	}

	for _, record := range records {
		if record.Error != "" && record.Output.StartedAt.IsZero() {
			continue
		}
		scheduledIndex := bucketIndex(record.Input.ScheduledAt, start, bucketSize, count)
		scheduled := &work[scheduledIndex]
		scheduled.ScheduledByTenant[record.Input.Tenant]++
		scheduled.ScheduledBySubtenant[subtenantKey(record.Input.Tenant, record.Input.Subtenant)]++
		scheduled.ScheduledServiceByTenant[record.Input.Tenant] += record.Input.PredictedCost
		scheduled.weightSums[record.Input.FairnessKey] += record.Input.FairnessWeight
		scheduled.weightCount[record.Input.FairnessKey]++
		scheduled.targetSums[record.Input.Tenant] += record.Input.TargetTenantShare
		scheduled.targetCount[record.Input.Tenant]++
		subtenant := subtenantKey(record.Input.Tenant, record.Input.Subtenant)
		scheduled.targetSubtenantSums[subtenant] += record.Input.TargetTenantShare * record.Input.TargetSubtenantShare
		scheduled.targetSubtenantCount[subtenant]++
		if record.Input.FairnessWeight != record.Input.UnclampedWeight {
			scheduled.ClampedWeights++
		}

		if record.Error != "" || record.Output.StartedAt.IsZero() {
			continue
		}
		startedIndex := bucketIndex(record.Output.StartedAt, start, bucketSize, count)
		started := &work[startedIndex]
		started.StartedByTenant[record.Input.Tenant]++
		started.StartedBySubtenant[subtenantKey(record.Input.Tenant, record.Input.Subtenant)]++
		started.ServiceByTenant[record.Input.Tenant] += record.Input.PredictedCost
		started.ServiceBySubtenant[subtenantKey(record.Input.Tenant, record.Input.Subtenant)] += record.Input.PredictedCost
		started.latencies = append(started.latencies, float64(record.Output.StartedAt.Sub(record.Input.ScheduledAt))/float64(time.Millisecond))
	}

	outstanding := 0
	outstandingByTenant := make(map[string]int)
	outstandingBySubtenant := make(map[string]int)
	for i := range work {
		bucket := &work[i]
		for tenant, n := range bucket.ScheduledByTenant {
			outstanding += n
			outstandingByTenant[tenant] += n
		}
		for subtenant, n := range bucket.ScheduledBySubtenant {
			outstandingBySubtenant[subtenant] += n
		}
		for tenant, n := range bucket.StartedByTenant {
			outstanding -= n
			outstandingByTenant[tenant] -= n
		}
		for subtenant, n := range bucket.StartedBySubtenant {
			outstandingBySubtenant[subtenant] -= n
		}
		bucket.OutstandingApprox = max(0, outstanding)
		for tenant, n := range outstandingByTenant {
			bucket.OutstandingByTenant[tenant] = max(0, n)
		}
		for subtenant, n := range outstandingBySubtenant {
			bucket.OutstandingBySubtenant[subtenant] = max(0, n)
		}
		for key, sum := range bucket.weightSums {
			bucket.AverageWeightByKey[key] = sum / float64(bucket.weightCount[key])
		}
		for tenant, sum := range bucket.targetSums {
			bucket.TargetShareByTenant[tenant] = sum / float64(bucket.targetCount[tenant])
		}
		for subtenant, sum := range bucket.targetSubtenantSums {
			bucket.TargetShareBySubtenant[subtenant] = sum / float64(bucket.targetSubtenantCount[subtenant])
		}
		slices.Sort(bucket.latencies)
		bucket.ScheduleToStartP50MS = percentile(bucket.latencies, 0.50)
		bucket.ScheduleToStartP95MS = percentile(bucket.latencies, 0.95)
		analysis.Buckets = append(analysis.Buckets, bucket.Bucket)
	}
	return analysis
}

func WriteJSONL(path string, records []Record) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create JSONL: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, record := range records {
		line, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return fmt.Errorf("encode record: %w", marshalErr)
		}
		if _, err = writer.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("write record: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush JSONL: %w", err)
	}
	return nil
}

func ReadJSONL(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open JSONL: %w", err)
	}
	defer file.Close()

	var records []Record
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode JSONL record %d: %w", len(records)+1, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read JSONL: %w", err)
	}
	return records, nil
}

func newBucketWork(index int, start time.Time) bucketWork {
	return bucketWork{
		Bucket: Bucket{
			Index:                    index,
			Start:                    start,
			ScheduledByTenant:        make(map[string]int),
			ScheduledBySubtenant:     make(map[string]int),
			ScheduledServiceByTenant: make(map[string]float64),
			StartedByTenant:          make(map[string]int),
			StartedBySubtenant:       make(map[string]int),
			ServiceByTenant:          make(map[string]float64),
			ServiceBySubtenant:       make(map[string]float64),
			TargetShareByTenant:      make(map[string]float64),
			TargetShareBySubtenant:   make(map[string]float64),
			AverageWeightByKey:       make(map[string]float64),
			OutstandingByTenant:      make(map[string]int),
			OutstandingBySubtenant:   make(map[string]int),
		},
		weightSums:           make(map[string]float64),
		weightCount:          make(map[string]int),
		targetSums:           make(map[string]float64),
		targetCount:          make(map[string]int),
		targetSubtenantSums:  make(map[string]float64),
		targetSubtenantCount: make(map[string]int),
	}
}

func bucketIndex(at, start time.Time, size time.Duration, count int) int {
	index := int(at.Sub(start) / size)
	return min(count-1, max(0, index))
}

func percentile(sorted []float64, quantile float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(quantile*float64(len(sorted)-1))]
}

func subtenantKey(tenant, subtenant string) string {
	return tenant + "/" + subtenant
}
