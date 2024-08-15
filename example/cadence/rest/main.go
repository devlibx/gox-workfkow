package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/serialization"
	"github.com/devlibx/gox-workfkow/workflow/framework/cadence"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	"github.com/google/uuid"
	errors2 "github.com/pkg/errors"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/workflow"
	"net/http"
	"os"
	"time"
)

//go:embed config.yaml
var config string

type requestPojo struct {
	TimeInMs int `json:"time_in_ms"`
	Counter  int `json:"counter"`
}

type responsePojo struct {
	Status  string `json:"status"`
	Counter int    `json:"counter"`
}

func main() {

	// Start server
	mainServer()
	time.Sleep(2 * time.Second)

	// Make http call to get response
	resp, err := resty.New().R().SetBody(requestPojo{TimeInMs: 1000}).Post("http://localhost:14567/api/v1/test")
	if err != nil {
		panic(err)
	} else {
		println(resp.String())
	}

	time.Sleep(60 * time.Second)
}

func mainServer() {

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
	we := &workflowBaseService{}
	workflow.Register(we.getHomeDataWorkflow)
	activity.Register(we.getHomeDataActivity)

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

	// Setup Http server
	router := gin.Default()
	router.POST("/api/v1/test", func(c *gin.Context) {

		req := requestPojo{}
		err := c.BindJSON(&req)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if res, err := we.getHomeData(req); err == nil {
			c.JSON(http.StatusOK, res)
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	var server *http.Server
	go func() {
		server = &http.Server{
			Addr:    ":14567",
			Handler: router,
		}
		if err := server.ListenAndServe(); err != nil && !errors2.Is(err, http.ErrServerClosed) {
			fmt.Printf("ListenAndServe() error: %v\n", err)
		}
	}()

	go func() {
		time.Sleep(20 * time.Second)
		_ = server.Shutdown(context.Background())
	}()
}

type workflowBaseService struct {
	cadenceApi cadence.Api
}

// getHomeData this is an api which will return the home page data
//
// This is designed to get sync response from cadence workflow if it is completed. If not return PROCESSING response
// This way you can make sure that you are not blocking the http call, and if it takes time then you can return PROCESSING
func (w *workflowBaseService) getHomeData(request requestPojo) (*responsePojo, error) {

	id := uuid.New().String()
	workflowOptions := client.StartWorkflowOptions{
		ID:                              id,
		TaskList:                        "server_2_ts_1",
		ExecutionStartToCloseTimeout:    10 * time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Minute,
	}

	// Start the workflow to get home page data
	workflowResp, err := w.cadenceApi.StartWorkflow(context.Background(), workflowOptions, w.getHomeDataWorkflow, request)
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelFunc()
	for {
		select {
		case <-timeoutCtx.Done():
			return &responsePojo{Status: "PROCESSING"}, nil

		case <-ticker.C:
			var result *responsePojo
			if queryResult, err := w.cadenceApi.QueryWorkflow(
				context.WithValue(context.Background(), cadence.TaskListForAction, "server_2_ts_1"),
				workflowResp.ID,
				workflowResp.RunID,
				"exampleQuery",
			); err == nil {
				if err := queryResult.Get(&result); err == nil {
					return result, nil
				}
			}
		}
	}
}

func (w *workflowBaseService) getHomeDataWorkflow(ctx workflow.Context, request requestPojo) (*responsePojo, error) {

	// Set up the query handler
	var result *responsePojo
	err := workflow.SetQueryHandler(ctx, "exampleQuery", func() (*responsePojo, error) {
		if result != nil {
			result.Status = result.Status + "-from query"
			return result, nil
		}
		return nil, errors.New("data not found")
	})
	if err != nil {
		return result, err
	}

	registerActivityOption := workflow.ActivityOptions{
		TaskList:               "server_2_ts_1",
		ScheduleToCloseTimeout: 30 * time.Minute,
		ScheduleToStartTimeout: 30 * time.Minute,
		StartToCloseTimeout:    30 * time.Minute,
		HeartbeatTimeout:       10 * time.Minute,
		WaitForCancellation:    false,
	}
	ctx = workflow.WithActivityOptions(ctx, registerActivityOption)

	// Simulate a retry loop - here we will get success in 2nd attempt
	// But this will simulate a much longer workflow - but also the result is ready so the http call will not block
	// Http call will use Query support to get the result
	for i := 0; i < 10; i++ {
		var loopResult *responsePojo
		request.Counter = i
		if err := workflow.ExecuteActivity(ctx, w.getHomeDataActivity, request).Get(ctx, &loopResult); err == nil {
			result = loopResult
		}
		_ = workflow.Sleep(ctx, 1*time.Second)
	}
	return result, nil
}

// getHomeDataActivity simulates an api call to get home data - which can take time
func (w *workflowBaseService) getHomeDataActivity(ctx context.Context, request requestPojo) (*responsePojo, error) {
	time.Sleep(time.Duration(request.TimeInMs) * time.Millisecond)
	if request.Counter == 5 {
		return &responsePojo{Status: "ok", Counter: request.Counter}, nil
	} else {
		return nil, fmt.Errorf("simulated error - counter=%d", request.Counter)
	}
}
