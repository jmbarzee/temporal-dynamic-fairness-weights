package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/temporalio/scratch-fairness-weights/internal/config"
	"github.com/temporalio/scratch-fairness-weights/internal/experiment"
	"github.com/temporalio/scratch-fairness-weights/internal/report"
	"github.com/temporalio/scratch-fairness-weights/internal/results"
	"go.temporal.io/sdk/client"
)

func main() {
	var (
		suitePath    = flag.String("suite", "scenarios/suite.json", "scenario suite JSON file")
		scenarioPath = flag.String("scenario", "", "run one scenario JSON file instead of the suite")
		outputDir    = flag.String("out", "out", "output directory")
		address      = flag.String("address", envOr("TEMPORAL_ADDRESS", client.DefaultHostPort), "Temporal frontend address")
		namespace    = flag.String("namespace", envOr("TEMPORAL_NAMESPACE", client.DefaultNamespace), "Temporal namespace")
		reportOnly   = flag.Bool("report-only", false, "regenerate reports from existing JSONL files without running Activities")
	)
	flag.Parse()

	if err := run(*suitePath, *scenarioPath, *outputDir, *address, *namespace, *reportOnly); err != nil {
		log.Fatal(err)
	}
}

func run(suitePath, scenarioPath, outputDir, address, namespace string, reportOnly bool) error {
	var (
		suiteName string
		configs   []config.Runtime
	)
	if scenarioPath != "" {
		cfg, err := config.Load(scenarioPath)
		if err != nil {
			return err
		}
		suiteName = cfg.Name
		configs = []config.Runtime{cfg}
	} else {
		suite, loadedConfigs, err := config.LoadSuite(suitePath)
		if err != nil {
			return err
		}
		suiteName = suite.Name
		configs = loadedConfigs
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	if reportOnly {
		var scenarioResults []report.ScenarioResult
		for _, cfg := range configs {
			records, readErr := results.ReadJSONL(filepath.Join(outputDir, cfg.Name, experiment.ModeHierarchical+".jsonl"))
			if readErr != nil {
				return readErr
			}
			analyses := []results.Analysis{results.Analyze(experiment.ModeHierarchical, records, cfg.Bucket)}
			scenarioResults = append(scenarioResults, report.ScenarioResult{Config: cfg, Analyses: analyses})
		}
		return writeReports(outputDir, suiteName, scenarioResults)
	}

	temporalClient, err := client.Dial(client.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		return fmt.Errorf("connect to Temporal at %s: %w", address, err)
	}
	defer temporalClient.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runID := time.Now().UTC().Format("20060102T150405")
	var scenarioResults []report.ScenarioResult
	for _, cfg := range configs {
		scenarioDir := filepath.Join(outputDir, cfg.Name)
		if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
			return fmt.Errorf("create scenario output directory: %w", err)
		}
		mode := experiment.ModeHierarchical
		taskQueue := strings.Join([]string{"fairness-lab", runID, cfg.Name, mode}, "-")
		log.Printf("running %s on task queue %s", cfg.Name, taskQueue)
		records, runErr := experiment.Run(ctx, temporalClient, cfg, mode, taskQueue, runID+"-"+cfg.Name)
		if runErr != nil {
			return fmt.Errorf("run %s experiment: %w", cfg.Name, runErr)
		}
		jsonlPath := filepath.Join(scenarioDir, mode+".jsonl")
		if err := results.WriteJSONL(jsonlPath, records); err != nil {
			return err
		}
		analysis := results.Analyze(mode, records, cfg.Bucket)
		analyses := []results.Analysis{analysis}
		log.Printf("%s: %d completed, %d failed", cfg.Name, analysis.Successful, analysis.Failed)
		scenarioResults = append(scenarioResults, report.ScenarioResult{Config: cfg, Analyses: analyses})
	}

	return writeReports(outputDir, suiteName, scenarioResults)
}

func writeReports(outputDir, suiteName string, scenarios []report.ScenarioResult) error {
	reportPath := filepath.Join(outputDir, "report.html")
	if err := report.WriteSuite(reportPath, suiteName, scenarios); err != nil {
		return err
	}
	summaryPath := filepath.Join(outputDir, "analysis.json")
	if err := report.WriteSuiteJSON(summaryPath, suiteName, scenarios); err != nil {
		return err
	}
	log.Printf("wrote %s", reportPath)
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
