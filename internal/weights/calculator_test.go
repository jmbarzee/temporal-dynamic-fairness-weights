package weights

import (
	"math"
	"testing"
	"time"
)

func TestEqualPolicyNormalizesActiveSiblings(t *testing.T) {
	calculator, err := NewCalculator(time.Minute, 0, 0.25, 10)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := calculator.ObserveAndCalculate(now, "tenant", "a", 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := calculator.ObserveAndCalculate(now, "tenant", "b", 2, 1); err != nil {
		t.Fatal(err)
	}
	decision, err := calculator.ObserveAndCalculate(now, "tenant", "a", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Share != 0.5 {
		t.Fatalf("expected equal share 0.5, got %v", decision.Share)
	}
	if decision.Weight != 10 {
		t.Fatalf("expected weight 10, got %v", decision.Weight)
	}
}

func TestVolumePolicyUsesNormalizedScores(t *testing.T) {
	calculator, err := NewCalculator(time.Minute, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for range 3 {
		if _, err := calculator.ObserveAndCalculate(now, "tenant", "hot", 1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := calculator.ObserveAndCalculate(now, "tenant", "cold", 1, 1); err != nil {
		t.Fatal(err)
	}
	decision, err := calculator.ObserveAndCalculate(now, "tenant", "hot", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := 5.0 / 7.0
	if math.Abs(decision.Share-want) > 1e-9 {
		t.Fatalf("expected share %v, got %v", want, decision.Share)
	}
}

func TestExpiredSiblingStopsParticipating(t *testing.T) {
	calculator, err := NewCalculator(time.Second, 0, 0.25, 1)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if _, err := calculator.ObserveAndCalculate(now, "tenant", "old", 1, 1); err != nil {
		t.Fatal(err)
	}
	decision, err := calculator.ObserveAndCalculate(now.Add(2*time.Second), "tenant", "active", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Share != 1 || decision.ActiveSiblings != 1 {
		t.Fatalf("expected sole active sibling, got %+v", decision)
	}
}

func TestCostAdjustmentAndClamping(t *testing.T) {
	calculator, err := NewCalculator(time.Minute, 0, 0.25, 10)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	decision, err := calculator.ObserveAndCalculate(now, "normal", "only", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Weight != 5 {
		t.Fatalf("expected cost-adjusted weight 5, got %v", decision.Weight)
	}
	low, err := calculator.ObserveAndCalculate(now, "low", "only", 1, 1e9)
	if err != nil {
		t.Fatal(err)
	}
	if low.Weight != MinWeight || !low.Clamped {
		t.Fatalf("expected minimum clamp, got %+v", low)
	}
	high, err := calculator.ObserveAndCalculate(now, "high", "only", 1e9, 1)
	if err != nil {
		t.Fatal(err)
	}
	if high.Weight != MaxWeight || !high.Clamped {
		t.Fatalf("expected maximum clamp, got %+v", high)
	}
}
