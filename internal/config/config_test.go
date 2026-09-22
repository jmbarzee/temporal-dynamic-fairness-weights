package config

import (
	"testing"
	"time"
)

func TestLoadProfileRate(t *testing.T) {
	profile := LoadProfile{
		BaseRate: 2,
		Cap:      20,
		Wake: Wake{
			Start:      0.2,
			End:        0.8,
			Smoothness: 0.03,
			Amplitude:  10,
		},
		Bumps: []Bump{{Center: 0.5, Width: 0.05, Amplitude: 20}},
	}

	overnight := profile.Rate(0)
	morning := profile.Rate(0.35)
	peak := profile.Rate(0.5)
	evening := profile.Rate(1)
	if morning <= overnight || morning <= evening {
		t.Fatalf("wake curve did not rise and fall: overnight=%v morning=%v evening=%v", overnight, morning, evening)
	}
	if peak != profile.Cap {
		t.Fatalf("expected peak to be capped at %v, got %v", profile.Cap, peak)
	}
}

func TestLoadProfilePrimitives(t *testing.T) {
	profile := LoadProfile{
		Saws:      []Saw{{Cycles: 1, Low: 0, High: 10}},
		Plateaus:  []Plateau{{Start: 0.2, End: 0.4, EdgeWidth: 0.01, Amplitude: 10}},
		Triangles: []Triangle{{Start: 0.5, Peak: 0.6, End: 0.7, Amplitude: 20}},
	}
	if got := profile.Rate(0.1); got < 0.9 || got > 1.1 {
		t.Fatalf("unexpected saw value: %v", got)
	}
	if got := profile.Rate(0.3); got < 12 {
		t.Fatalf("plateau did not contribute: %v", got)
	}
	if got := profile.Rate(0.6); got < 25 {
		t.Fatalf("triangle did not peak: %v", got)
	}
}

func TestSuiteScenariosSpendBoundedTimeContended(t *testing.T) {
	_, scenarios, err := LoadSuite("../../scenarios/suite.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			fraction, remaining := modeledContention(scenario)
			t.Logf("contention %.1f%%, remaining %.1f", fraction*100, remaining)
			if fraction < 0.20 || fraction > 0.60 {
				t.Fatalf("contention fraction %.1f%% outside [20%%, 60%%]", fraction*100)
			}
			if remaining > 1 {
				t.Fatalf("scenario ends with %.1f modeled work still backlogged", remaining)
			}
		})
	}
}

func modeledContention(cfg Runtime) (fraction float64, remaining float64) {
	const step = 100 * time.Millisecond
	batchFired := make([][]bool, len(cfg.Streams))
	for i, stream := range cfg.Streams {
		batchFired[i] = make([]bool, len(stream.Profile.Batches))
	}
	var contended int
	var total int
	for elapsed := time.Duration(0); elapsed < cfg.DurationValue; elapsed += step {
		progress := elapsed.Seconds() / cfg.DurationValue.Seconds()
		var offeredWork float64
		for i, stream := range cfg.Streams {
			offeredWork += stream.Profile.Rate(progress) * stream.Cost * step.Seconds()
			for batchIndex, batch := range stream.Profile.Batches {
				if !batchFired[i][batchIndex] && progress >= batch.At {
					batchFired[i][batchIndex] = true
					offeredWork += float64(batch.Tasks) * stream.Cost
				}
			}
		}
		remaining = max(0, remaining+offeredWork-cfg.ExpectedCapacity*step.Seconds())
		if remaining > 1 {
			contended++
		}
		total++
	}
	return float64(contended) / float64(total), remaining
}
