package weights

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	MinWeight = 0.001
	MaxWeight = 1000.0
)

type Decision struct {
	Weight          float64
	UnclampedWeight float64
	Share           float64
	RecentCount     int
	ActiveSiblings  int
	Clamped         bool
}

type Calculator struct {
	mu      sync.Mutex
	window  time.Duration
	alpha   float64
	epsilon float64
	scale   float64
	history map[string]map[string][]time.Time
}

func NewCalculator(window time.Duration, alpha, epsilon, scale float64) (*Calculator, error) {
	if window <= 0 || epsilon <= 0 || scale <= 0 {
		return nil, fmt.Errorf("window, epsilon, and scale must be positive")
	}
	return &Calculator{
		window:  window,
		alpha:   alpha,
		epsilon: epsilon,
		scale:   scale,
		history: make(map[string]map[string][]time.Time),
	}, nil
}

func (c *Calculator) ObserveAndCalculate(
	now time.Time,
	tenant string,
	subtenant string,
	tenantWeight float64,
	predictedCost float64,
) (Decision, error) {
	if tenant == "" || subtenant == "" || tenantWeight <= 0 || predictedCost <= 0 ||
		math.IsNaN(tenantWeight) || math.IsNaN(predictedCost) ||
		math.IsInf(tenantWeight, 0) || math.IsInf(predictedCost, 0) {
		return Decision{}, fmt.Errorf("invalid tenant, subtenant, weight, or cost")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	bySubtenant := c.history[tenant]
	if bySubtenant == nil {
		bySubtenant = make(map[string][]time.Time)
		c.history[tenant] = bySubtenant
	}
	cutoff := now.Add(-c.window)
	for key, observations := range bySubtenant {
		first := 0
		for first < len(observations) && observations[first].Before(cutoff) {
			first++
		}
		if first == len(observations) {
			delete(bySubtenant, key)
			continue
		}
		bySubtenant[key] = observations[first:]
	}
	bySubtenant[subtenant] = append(bySubtenant[subtenant], now)

	var totalScore float64
	for _, observations := range bySubtenant {
		totalScore += math.Pow(float64(len(observations))+c.epsilon, c.alpha)
	}
	recentCount := len(bySubtenant[subtenant])
	score := math.Pow(float64(recentCount)+c.epsilon, c.alpha)
	share := score / totalScore
	unclamped := c.scale * tenantWeight * share / predictedCost
	weight := min(MaxWeight, max(MinWeight, unclamped))

	return Decision{
		Weight:          weight,
		UnclampedWeight: unclamped,
		Share:           share,
		RecentCount:     recentCount,
		ActiveSiblings:  len(bySubtenant),
		Clamped:         weight != unclamped,
	}, nil
}
