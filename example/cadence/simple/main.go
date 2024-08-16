package main

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/devlibx/gox-base"
	config2 "github.com/devlibx/gox-base/config"
	"github.com/devlibx/gox-base/metrics"
	"github.com/devlibx/gox-base/serialization"
	stats "github.com/devlibx/gox-metrics/common"
	"github.com/devlibx/gox-workfkow/workflow/framework/cadence"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	errors2 "github.com/pkg/errors"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
	"log/slog"
	"math/rand"
	"net/http"
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

	ms, mh, err := stats.NewMetricService(&metrics.Config{Enabled: true, EnablePrometheus: true, Prefix: ""}, &config2.App{AppName: "a"})
	if err != nil {
		panic(err)
	}

	// Create a new cadence client
	workflowApi, err := cadence.NewCadenceClient(gox.NewCrossFunction(ms), &c)
	if err != nil {
		panic(err)
	}

	appCts, cff := context.WithTimeout(context.Background(), 10*time.Hour)
	defer cff()

	// Make sure to start it - mandatory to do it
	err = workflowApi.Start(appCts)
	if err != nil {
		panic(err)
	}
	we.cadenceApi = workflowApi

	go func() {
		runServer(mh)
	}()

	for i := 0; i < 2; i++ {
		we.RunExample()
		time.Sleep(500 * time.Millisecond)
	}

	time.Sleep(60 * time.Hour)
}

func runServer(mh *stats.MetricHandler) {
	router := gin.Default()
	router.GET("/metrics", func(c *gin.Context) {
		mh.MetricsReporter.HTTPHandler().ServeHTTP(c.Writer, c.Request)
	})
	server := &http.Server{
		Addr:    ":14567",
		Handler: router,
	}
	if err := server.ListenAndServe(); err != nil && !errors2.Is(err, http.ErrServerClosed) {
		fmt.Printf("ListenAndServe() error: %v\n", err)
	}
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
	if rand.Int()%3 == 0 {
		return gox.StringObjectMap{"status": "ok", "id": input}, fmt.Errorf("some bad error in printf %w", errors2.New("some bad error"))
	} else {
		return gox.StringObjectMap{"status": "ok", "id": input}, nil
	}
}
