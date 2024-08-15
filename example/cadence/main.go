package main

import (
	"context"
	"github.com/devlibx/gox-base"
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

	workflowApi, err := cadence.NewCadenceClient(gox.NewNoOpCrossFunction(), &c)
	if err != nil {
		panic(err)
	}

	err = workflowApi.Start(context.Background(), &c)
	if err != nil {
		panic(err)
	}
	we.cadenceApi = workflowApi

	we.RunExample()

	time.Sleep(60 * time.Second)
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

	registerActivityOption := workflow.ActivityOptions{
		TaskList:               "server_2_ts_1",
		ScheduleToCloseTimeout: 30 * time.Second,
		ScheduleToStartTimeout: 30 * time.Second,
		StartToCloseTimeout:    30 * time.Second,
		HeartbeatTimeout:       10 * time.Second,
		WaitForCancellation:    false,
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
	return gox.StringObjectMap{"status": "ok", "id": input}, nil
}
