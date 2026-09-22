package config

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

type Suite struct {
	Name      string   `json:"name"`
	Scenarios []string `json:"scenarios"`
}

type Config struct {
	Name                   string            `json:"name"`
	Description            string            `json:"description,omitempty"`
	Seed                   uint64            `json:"seed"`
	GatewayPods            int               `json:"gatewayPods"`
	HistoryWindow          string            `json:"historyWindow"`
	Alpha                  float64           `json:"alpha"`
	Epsilon                float64           `json:"epsilon"`
	WeightScale            float64           `json:"weightScale"`
	WorkerConcurrency      int               `json:"workerConcurrency"`
	BaseWorkDurationMillis int               `json:"baseWorkDurationMillis"`
	ExpectedCapacity       float64           `json:"expectedCapacity,omitempty"`
	BucketDuration         string            `json:"bucketDuration"`
	WarmupDuration         string            `json:"warmupDuration"`
	Tenants                map[string]Tenant `json:"tenants"`
	Duration               string            `json:"duration,omitempty"`
	Streams                []Stream          `json:"streams,omitempty"`
	Phases                 []Phase           `json:"phases,omitempty"`
}

type Tenant struct {
	Weight float64 `json:"weight"`
}

type Phase struct {
	Name     string   `json:"name"`
	Duration string   `json:"duration"`
	Streams  []Stream `json:"streams"`
}

type Stream struct {
	Tenant       string       `json:"tenant"`
	Subtenant    string       `json:"subtenant"`
	RequestsPerS float64      `json:"requestsPerSecond"`
	Cost         float64      `json:"cost"`
	Profile      *LoadProfile `json:"profile,omitempty"`
}

type LoadProfile struct {
	BaseRate  float64    `json:"baseRate"`
	Cap       float64    `json:"cap"`
	Wake      Wake       `json:"wake"`
	Bumps     []Bump     `json:"bumps,omitempty"`
	Rhythms   []Rhythm   `json:"rhythms,omitempty"`
	Saws      []Saw      `json:"saws,omitempty"`
	Plateaus  []Plateau  `json:"plateaus,omitempty"`
	Triangles []Triangle `json:"triangles,omitempty"`
	Batches   []Batch    `json:"batches,omitempty"`
}

type Wake struct {
	Start      float64 `json:"start"`
	End        float64 `json:"end"`
	Smoothness float64 `json:"smoothness"`
	Amplitude  float64 `json:"amplitude"`
}

type Bump struct {
	Center    float64 `json:"center"`
	Width     float64 `json:"width"`
	Amplitude float64 `json:"amplitude"`
}

type Rhythm struct {
	Cycles    float64 `json:"cycles"`
	Phase     float64 `json:"phase"`
	Amplitude float64 `json:"amplitude"`
}

type Saw struct {
	Cycles float64 `json:"cycles"`
	Phase  float64 `json:"phase"`
	Low    float64 `json:"low"`
	High   float64 `json:"high"`
}

type Plateau struct {
	Start     float64 `json:"start"`
	End       float64 `json:"end"`
	EdgeWidth float64 `json:"edgeWidth"`
	Amplitude float64 `json:"amplitude"`
}

type Triangle struct {
	Start     float64 `json:"start"`
	Peak      float64 `json:"peak"`
	End       float64 `json:"end"`
	Amplitude float64 `json:"amplitude"`
}

type Batch struct {
	At    float64 `json:"at"`
	Tasks int     `json:"tasks"`
}

type Runtime struct {
	Config
	HistoryWindowDuration time.Duration
	Bucket                time.Duration
	Warmup                time.Duration
	DurationValue         time.Duration
	PhaseDurations        []time.Duration
}

func Load(path string) (Runtime, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Runtime{}, fmt.Errorf("read scenario: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Runtime{}, fmt.Errorf("decode scenario: %w", err)
	}
	return cfg.runtime()
}

func LoadSuite(path string) (Suite, []Runtime, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Suite{}, nil, fmt.Errorf("read suite: %w", err)
	}
	var suite Suite
	if err := json.Unmarshal(data, &suite); err != nil {
		return Suite{}, nil, fmt.Errorf("decode suite: %w", err)
	}
	if suite.Name == "" || len(suite.Scenarios) == 0 {
		return Suite{}, nil, fmt.Errorf("suite name and scenarios are required")
	}
	runtimes := make([]Runtime, 0, len(suite.Scenarios))
	for _, scenario := range suite.Scenarios {
		if !filepath.IsAbs(scenario) {
			scenario = filepath.Join(filepath.Dir(path), scenario)
		}
		runtime, loadErr := Load(scenario)
		if loadErr != nil {
			return Suite{}, nil, loadErr
		}
		runtimes = append(runtimes, runtime)
	}
	return suite, runtimes, nil
}

func (c Config) runtime() (Runtime, error) {
	if c.Name == "" {
		return Runtime{}, fmt.Errorf("scenario name is required")
	}
	if c.GatewayPods < 1 || c.WorkerConcurrency < 1 {
		return Runtime{}, fmt.Errorf("gatewayPods and workerConcurrency must be positive")
	}
	if c.BaseWorkDurationMillis < 1 || c.WeightScale <= 0 || c.Epsilon <= 0 {
		return Runtime{}, fmt.Errorf("baseWorkDurationMillis, weightScale, and epsilon must be positive")
	}
	if len(c.Tenants) == 0 || (len(c.Phases) == 0 && len(c.Streams) == 0) {
		return Runtime{}, fmt.Errorf("at least one tenant and either phases or streams are required")
	}

	historyWindow, err := time.ParseDuration(c.HistoryWindow)
	if err != nil || historyWindow <= 0 {
		return Runtime{}, fmt.Errorf("invalid historyWindow %q", c.HistoryWindow)
	}
	bucket, err := time.ParseDuration(c.BucketDuration)
	if err != nil || bucket <= 0 {
		return Runtime{}, fmt.Errorf("invalid bucketDuration %q", c.BucketDuration)
	}
	warmup, err := time.ParseDuration(c.WarmupDuration)
	if err != nil || warmup < 0 {
		return Runtime{}, fmt.Errorf("invalid warmupDuration %q", c.WarmupDuration)
	}

	phaseDurations := make([]time.Duration, len(c.Phases))
	for i, phase := range c.Phases {
		duration, parseErr := time.ParseDuration(phase.Duration)
		if parseErr != nil || duration <= 0 {
			return Runtime{}, fmt.Errorf("invalid duration for phase %q", phase.Name)
		}
		phaseDurations[i] = duration
		for _, stream := range phase.Streams {
			tenant, ok := c.Tenants[stream.Tenant]
			if !ok {
				return Runtime{}, fmt.Errorf("phase %q references unknown tenant %q", phase.Name, stream.Tenant)
			}
			if tenant.Weight <= 0 || stream.Subtenant == "" || stream.RequestsPerS < 0 || stream.Cost <= 0 {
				return Runtime{}, fmt.Errorf("phase %q has invalid stream %+v", phase.Name, stream)
			}
		}
	}

	var duration time.Duration
	if len(c.Streams) > 0 {
		duration, err = time.ParseDuration(c.Duration)
		if err != nil || duration <= 0 {
			return Runtime{}, fmt.Errorf("invalid duration %q", c.Duration)
		}
		for _, stream := range c.Streams {
			tenant, ok := c.Tenants[stream.Tenant]
			if !ok {
				return Runtime{}, fmt.Errorf("stream references unknown tenant %q", stream.Tenant)
			}
			if tenant.Weight <= 0 || stream.Subtenant == "" || stream.Cost <= 0 || stream.Profile == nil {
				return Runtime{}, fmt.Errorf("invalid curved stream %+v", stream)
			}
			if err := stream.Profile.validate(); err != nil {
				return Runtime{}, fmt.Errorf("%s/%s: %w", stream.Tenant, stream.Subtenant, err)
			}
		}
	}

	return Runtime{
		Config:                c,
		HistoryWindowDuration: historyWindow,
		Bucket:                bucket,
		Warmup:                warmup,
		DurationValue:         duration,
		PhaseDurations:        phaseDurations,
	}, nil
}

func (p LoadProfile) Rate(progress float64) float64 {
	progress = min(1, max(0, progress))
	rate := p.BaseRate
	if p.Wake.Amplitude != 0 {
		rise := sigmoid((progress - p.Wake.Start) / p.Wake.Smoothness)
		fall := sigmoid((p.Wake.End - progress) / p.Wake.Smoothness)
		rate += p.Wake.Amplitude * rise * fall
	}
	for _, bump := range p.Bumps {
		distance := (progress - bump.Center) / bump.Width
		rate += bump.Amplitude * math.Exp(-0.5*distance*distance)
	}
	for _, rhythm := range p.Rhythms {
		rate += rhythm.Amplitude * math.Sin(2*math.Pi*(rhythm.Cycles*progress+rhythm.Phase))
	}
	for _, saw := range p.Saws {
		position := math.Mod(saw.Cycles*progress+saw.Phase, 1)
		if position < 0 {
			position++
		}
		rate += saw.Low + (saw.High-saw.Low)*position
	}
	for _, plateau := range p.Plateaus {
		rise := sigmoid((progress - plateau.Start) / plateau.EdgeWidth)
		fall := sigmoid((plateau.End - progress) / plateau.EdgeWidth)
		rate += plateau.Amplitude * rise * fall
	}
	for _, triangle := range p.Triangles {
		switch {
		case progress >= triangle.Start && progress < triangle.Peak:
			rate += triangle.Amplitude * (progress - triangle.Start) / (triangle.Peak - triangle.Start)
		case progress >= triangle.Peak && progress <= triangle.End:
			rate += triangle.Amplitude * (triangle.End - progress) / (triangle.End - triangle.Peak)
		}
	}
	rate = max(0, rate)
	if p.Cap > 0 {
		rate = min(p.Cap, rate)
	}
	return rate
}

func (p LoadProfile) validate() error {
	if p.BaseRate < 0 || p.Cap < 0 {
		return fmt.Errorf("invalid load profile")
	}
	if p.Wake.Amplitude != 0 && (p.Wake.Smoothness <= 0 ||
		p.Wake.Start < 0 || p.Wake.Start > 1 || p.Wake.End < 0 || p.Wake.End > 1 ||
		p.Wake.End <= p.Wake.Start) {
		return fmt.Errorf("invalid wake profile")
	}
	for _, bump := range p.Bumps {
		if bump.Center < 0 || bump.Center > 1 || bump.Width <= 0 {
			return fmt.Errorf("invalid bump %+v", bump)
		}
	}
	for _, rhythm := range p.Rhythms {
		if rhythm.Cycles <= 0 {
			return fmt.Errorf("invalid rhythm %+v", rhythm)
		}
	}
	for _, saw := range p.Saws {
		if saw.Cycles <= 0 || saw.Low < 0 || saw.High < saw.Low {
			return fmt.Errorf("invalid saw %+v", saw)
		}
	}
	for _, plateau := range p.Plateaus {
		if plateau.Start < 0 || plateau.End > 1 || plateau.End <= plateau.Start || plateau.EdgeWidth <= 0 {
			return fmt.Errorf("invalid plateau %+v", plateau)
		}
	}
	for _, triangle := range p.Triangles {
		if triangle.Start < 0 || triangle.End > 1 ||
			triangle.Peak <= triangle.Start || triangle.End <= triangle.Peak {
			return fmt.Errorf("invalid triangle %+v", triangle)
		}
	}
	for _, batch := range p.Batches {
		if batch.At < 0 || batch.At > 1 || batch.Tasks <= 0 {
			return fmt.Errorf("invalid batch %+v", batch)
		}
	}
	return nil
}

func sigmoid(value float64) float64 {
	return 1 / (1 + math.Exp(-value))
}
