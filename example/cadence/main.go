package main

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/google/uuid"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
	"gox-workfkow/workflow/framework/cadence"
	"log/slog"
	"os"
	"time"
)

func main() {

	c := cadence.Config{
		WorkerGroups: map[string]cadence.WorkerGroup{
			"worker_group_1": {
				Disabled: false,
				Name:     "server_1",
				Domain:   os.Getenv("TASK_LIST"),
				HostPort: os.Getenv("HOST"),
				Workers: []*cadence.Worker{
					{TaskList: "server_1_ts_1", WorkerCount: 3},
					{TaskList: "server_1_ts_2", WorkerCount: 3},
				},
			},
			"worker_group_2": {
				Name:     "server_2",
				Domain:   os.Getenv("TASK_LIST") + "-harishbohara",
				HostPort: os.Getenv("HOST"),
				Workers: []*cadence.Worker{
					{TaskList: "server_2_ts_1", WorkerCount: 3},
					{TaskList: "server_2_ts_2", WorkerCount: 3},
				},
			},
		},
	}

	we := &workflowExample{}
	workflow.Register(we.RunWorkflow)
	activity.Register(we.RunActivity)

	/*// Create a custom configuration
	config := zap.NewProductionConfig()
	config.EncoderConfig.StacktraceKey = ""                // Disable stack trace key
	config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel) // Set log level
	// logger, err := zap.NewProduction()
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}*/

	workflowApi, err := cadence.NewCadenceClient(gox.NewCrossFunction(), &c)
	if err != nil {
		panic(err)
	}

	err = workflowApi.Start(context.Background(), &c)
	if err != nil {
		panic(err)
	}
	we.cadenceApi = workflowApi

	for i := 0; i < 1; i++ {
		we.RunExample()
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(60 * time.Hour)
}

type workflowExample struct {
	cadenceApi cadence.Api
}

func (w *workflowExample) RunExample() {
	id := uuid.New().String()
	workflowOptions := client.StartWorkflowOptions{
		ID:                              id,
		TaskList:                        "server_2_ts_1",
		ExecutionStartToCloseTimeout:    10 * time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Minute,
	}

	result, err := w.cadenceApi.StartWorkflow(context.Background(), workflowOptions, w.RunWorkflow, "some-args-"+id)
	if err != nil {
		panic(err)
	}
	slog.Info("-->>> workflow started - ", slog.Any("result", result))
}

func (w *workflowExample) RunWorkflow(ctx workflow.Context, input string) error {

	retryPolicy := &workflow.RetryPolicy{
		InitialInterval:    time.Second, // Initial backoff interval
		BackoffCoefficient: 2.0,         // Exponential backoff coefficient
		MaximumAttempts:    100,         // Maximum number of attempts
		MaximumInterval:    5 * time.Second,
	}

	registerActivityOption := workflow.ActivityOptions{
		TaskList:               "server_2_ts_1",
		ScheduleToCloseTimeout: 30 * time.Minute,
		ScheduleToStartTimeout: 30 * time.Minute,
		StartToCloseTimeout:    30 * time.Minute,
		HeartbeatTimeout:       10 * time.Minute,
		WaitForCancellation:    false,
		RetryPolicy:            retryPolicy,
	}
	ctx = workflow.WithActivityOptions(ctx, registerActivityOption)

	result := gox.StringObjectMap{}
	if err := workflow.ExecuteActivity(ctx, w.RunActivity, input).Get(ctx, &result); err == nil {
		slog.Info("activity result - ", slog.Any("result", result))
		return nil
	}

	return nil
}

func (w *workflowExample) RunActivity(ctx context.Context, input string) (gox.StringObjectMap, error) {
	slog.Info("-->>>> Running activity - ", slog.String("input", input))
	return gox.StringObjectMap{"status": "ok", "id": input}, fmt.Errorf("some bad error in printf %w", errors.New("some bad error"))
	//errors.New("some bad error")
}
