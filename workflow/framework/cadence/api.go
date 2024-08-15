package cadence

import (
	"context"
	"github.com/devlibx/gox-base"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/workflow"
)

const TaskListForAction = "__task_list_for_action__"

// Config is the configuration for Cadence worker
type Config struct {
	EnableErrorStackInCadenceLog bool                   `json:"enable_error_stack_in_cadence_log" yaml:"enable_error_stack_in_cadence_log"`
	Disabled                     bool                   `json:"disabled" yaml:"disabled"`
	WorkerGroups                 map[string]WorkerGroup `json:"worker_groups" yaml:"worker_groups"`
}

// WorkerGroup is the configuration for Cadence worker group. It allows application to use more than one cadence
// server.
// Inside each server there can be multiple workers. Each worker can have multiple worker threads.
type WorkerGroup struct {
	Disabled bool      `json:"disabled" yaml:"disabled"`
	Name     string    `json:"name" yaml:"name"`
	Domain   string    `json:"domain" yaml:"domain"`
	HostPort string    `json:"host_port" yaml:"host_port"`
	Workers  []*Worker `json:"worker" yaml:"worker"`
}

// Worker is the configuration for Cadence worker
type Worker struct {
	Disabled    bool   `json:"disabled" yaml:"disabled"`
	TaskList    string `json:"task_list" yaml:"task_list"`
	WorkerCount int    `json:"worker_count" yaml:"worker_count"`
}

// Api is the interface for Cadence client. It is used to avoid direct dependency on Cadence client in the application code.
// This allows to make it easier to mock Cadence client in the tests.
type Api interface {

	// Start starts the Cadence client
	Start(ctx context.Context) error

	// Shutdown stops the Cadence client
	// This is a blocking call and done channel is used to notify when the shutdown is complete
	Shutdown(ctx context.Context) (chan error, error)

	// StartWorkflow starts a new workflow execution
	StartWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflowFunc interface{}, args ...interface{}) (*workflow.Execution, error)

	// ExecuteWorkflow starts a new workflow execution and return a WorkflowRun
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)

	// CancelWorkflow cancels a workflow execution
	//
	// IMPORTANT REQUIREMENT:
	// Since this is a cadence wrapper, you will have to pass the task list name to perform the action.
	//
	// e.g.
	// ctx := context.WithValue(context.Background(), cadence.TaskListForAction, "server_2_ts_1")
	CancelWorkflow(ctx context.Context, workflowID string, runID string) error

	// QueryWorkflow queries a workflow execution
	//
	// IMPORTANT REQUIREMENT:
	// Since this is a cadence wrapper, you will have to pass the task list name to perform the action.
	//
	// e.g.
	// ctx := context.WithValue(context.Background(), cadence.TaskListForAction, "server_2_ts_1")
	QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (encoded.Value, error)

	// TerminateWorkflow queries a workflow execution
	//
	// IMPORTANT REQUIREMENT:
	// Since this is a cadence wrapper, you will have to pass the task list name to perform the action.
	//
	// e.g.
	// ctx := context.WithValue(context.Background(), cadence.TaskListForAction, "server_2_ts_1")
	TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details []byte) error
}

func NewCadenceClient(cf gox.CrossFunction, config *Config) (Api, error) {
	impl := &cadenceWrapperImpl{
		CrossFunction: cf,
		config:        config,
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return impl, nil
}
