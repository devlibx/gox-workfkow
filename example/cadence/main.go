package main

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-workfkow/workflow/framework/cadence"
	"github.com/google/uuid"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
	"log/slog"
	"os"
	"time"
)

//go:embed config.yaml
var config string

func main() {

	// Make sure to set the env variables
	if os.Getenv("HOST") == "" || os.Getenv("TASK_LIST") == "" {
		panic("HOST and TASK_LIST are mandatory - HOST=<your cadence host:port> TASK_LIST=<your task list>")
	}

	// Read config file into config object
	config = os.ExpandEnv(config)
	c := cadence.Config{}
	err := serialization.ReadYamlFromString(config, &c)
	if err != nil {
		panic(err)
	}

	// Make sure to register workflow and activity before you start the cadence client
	we := &workflowExample{}
	workflow.Register(we.RunWorkflow)
	activity.Register(we.RunActivity)

	// Create a new cadence client
	workflowApi, err := cadence.NewCadenceClient(gox.NewCrossFunction(), &c)
	if err != nil {
		panic(err)
	}

	// Make sure to start it - mandatory to do it
	err = workflowApi.Start(context.Background())
	if err != nil {
		panic(err)
	}
	we.cadenceApi = workflowApi

	we.RunExample()
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

	_, err := w.cadenceApi.StartWorkflow(context.Background(), workflowOptions, w.RunWorkflow, "some-args-"+id)
	if err != nil {
		panic(err)
	}
}

func (w *workflowExample) RunWorkflow(ctx workflow.Context, input string) error {

	// Make sure to put correct retries
	retryPolicy := &workflow.RetryPolicy{
		InitialInterval:    time.Second,
		BackoffCoefficient: 1.1,
		MaximumAttempts:    3,
		MaximumInterval:    2 * time.Second,
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
}
