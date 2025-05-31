package main

import (
	"bufio"
	"context"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devlibx/gox-base/v2"
	config2 "github.com/devlibx/gox-base/v2/config"
	"github.com/devlibx/gox-base/v2/metrics"
	"github.com/devlibx/gox-base/v2/serialization"
	stats "github.com/devlibx/gox-metrics/v2/common"
	"github.com/devlibx/gox-workfkow/workflow/framework/cadence"
)

//go:embed config.yaml
var configTemplate string

const (
	opStatus    = "status"
	opTerminate = "terminate"
)

func main() {
	// Define command line arguments
	domain := flag.String("domain", "", "Cadence domain name")
	taskList := flag.String("taskList", "", "Task list name")
	serverURL := flag.String("serverURL", "", "Cadence server URL (host:port)")
	concurrency := flag.Int("concurrency", 1, "Number of concurrent workers")
	operation := flag.String("operation", opStatus, "Operation to perform: status or terminate")
	inputFile := flag.String("inputFile", "", "File containing workflow IDs (one per line)")
	outputFile := flag.String("outputFile", "workflow_results.txt", "Output file to store results")
	flag.Parse()

	// Validate required parameters
	if *domain == "" || *taskList == "" || *serverURL == "" || *inputFile == "" {
		fmt.Println("Required parameters missing. Usage:")
		fmt.Println("  -domain      Cadence domain name (required)")
		fmt.Println("  -taskList    Task list name (required)")
		fmt.Println("  -serverURL   Cadence server URL, host:port (required)")
		fmt.Println("  -concurrency Number of concurrent workers (default: 1)")
		fmt.Println("  -operation   Operation to perform: status or terminate (default: status)")
		fmt.Println("  -inputFile   File containing workflow IDs, one per line (required)")
		fmt.Println("  -outputFile  Output file to store results (default: workflow_results.txt)")
		os.Exit(1)
	}

	// Validate operation
	if *operation != opStatus && *operation != opTerminate {
		fmt.Printf("Invalid operation: %s. Must be either 'status' or 'terminate'\n", *operation)
		os.Exit(1)
	}

	// Set up environment variables for config template
	os.Setenv("DOMAIN", *domain)
	os.Setenv("HOST", *serverURL)
	os.Setenv("TASK_LIST", *taskList)

	// Read config file into config object
	configStr := os.ExpandEnv(configTemplate)
	c := cadence.Config{}
	err := serialization.ReadYamlFromString(configStr, &c)
	if err != nil {
		panic(fmt.Errorf("failed to parse config: %w", err))
	}

	// Set up metrics service
	ms, _, err := stats.NewMetricService(&metrics.Config{Enabled: true, EnablePrometheus: true, Prefix: ""}, &config2.App{AppName: "cadence-status-check"})
	if err != nil {
		panic(fmt.Errorf("failed to create metric service: %w", err))
	}

	// Create a new cadence client
	workflowApi, err := cadence.NewCadenceClient(gox.NewCrossFunction(ms), &c)
	if err != nil {
		panic(fmt.Errorf("failed to create cadence client: %w", err))
	}

	// Setup context with timeout
	appCtx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
	defer cancel()

	// Start the cadence client
	err = workflowApi.Start(appCtx)
	if err != nil {
		panic(fmt.Errorf("failed to start cadence client: %w", err))
	}

	// Create output file for writing results
	out, err := os.Create(*outputFile)
	if err != nil {
		panic(fmt.Errorf("failed to create output file: %w", err))
	}
	defer out.Close()

	// Write header to the file based on operation
	if *operation == opStatus {
		_, err = fmt.Fprintf(out, "Workflow ID\tStatus\tStatus Description\n")
	} else {
		_, err = fmt.Fprintf(out, "Workflow ID\tTermination Status\n")
	}
	if err != nil {
		panic(fmt.Errorf("failed to write header to output file: %w", err))
	}

	// Open and read the input file
	input, err := os.Open(*inputFile)
	if err != nil {
		panic(fmt.Errorf("failed to open input file: %w", err))
	}
	defer input.Close()

	// Process workflows
	scanner := bufio.NewScanner(input)
	var count atomic.Uint64
	ch := make(chan string, 1000)
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	// Start worker goroutines
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for workflowID := range ch {
				if *operation == opStatus {
					processStatusCheck(workflowApi, *taskList, workflowID, out, &count, mu)
				} else {
					processTermination(workflowApi, *taskList, workflowID, out, &count, mu)
				}
			}
		}()
	}

	// Feed workflow IDs to workers
	for scanner.Scan() {
		ch <- scanner.Text()
	}
	close(ch)

	// Wait for all workers to finish
	wg.Wait()

	slog.Info("Operation completed",
		slog.String("operation", *operation),
		slog.Uint64("total_workflows", count.Load()),
		slog.String("output_file", *outputFile))
}

// processStatusCheck performs status check on a workflow
func processStatusCheck(api cadence.Api, taskList, workflowID string, outputFile *os.File, counter *atomic.Uint64, mu *sync.Mutex) {
	status, statusString := cadence.GetCadenceWorkflowStatus(api, taskList, workflowID, "")
	counter.Add(1)
	
	slog.Info("Workflow status check",
		slog.Uint64("count", counter.Load()),
		slog.String("id", workflowID),
		slog.String("status", status),
		slog.String("description", statusString))
	
	// Write to file with mutex protection
	mu.Lock()
	_, err := fmt.Fprintf(outputFile, "%s\t%s\t%s\n", workflowID, status, statusString)
	mu.Unlock()
	
	if err != nil {
		slog.Error("Failed to write to output file", slog.String("error", err.Error()))
	}
}

// processTermination terminates a workflow
func processTermination(api cadence.Api, taskList, workflowID string, outputFile *os.File, counter *atomic.Uint64, mu *sync.Mutex) {
	ctx := cadence.ContextWithTaskListInfo(context.Background(), taskList)
	err := api.TerminateWorkflow(ctx, workflowID, "", "Terminated by request", []byte("terminated"))
	counter.Add(1)
	
	status := "SUCCESS"
	if err != nil {
		status = "FAILED"
		slog.Error("Workflow termination failed",
			slog.Uint64("count", counter.Load()),
			slog.String("id", workflowID),
			slog.String("error", err.Error()))
	} else {
		slog.Info("Workflow terminated",
			slog.Uint64("count", counter.Load()),
			slog.String("id", workflowID))
	}
	
	// Write to file with mutex protection
	mu.Lock()
	_, err = fmt.Fprintf(outputFile, "%s\t%s\n", workflowID, status)
	mu.Unlock()
	
	if err != nil {
		slog.Error("Failed to write to output file", slog.String("error", err.Error()))
	}
}