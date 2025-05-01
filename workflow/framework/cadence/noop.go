package cadence

import (
	"context"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/google/uuid"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/workflow"
	"log/slog"
)

type noOpCadenceApi struct {
}

func (n noOpCadenceApi) Start(ctx context.Context) error {
	slog.Warn("************** Using NO OP IMPL for Cadence - Start ************** ")
	return nil
}

func (n noOpCadenceApi) Shutdown(ctx context.Context) (chan error, error) {
	ch := make(chan error, 2)
	ch <- nil
	close(ch)
	return ch, nil
}

func (n noOpCadenceApi) StartWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflowFunc interface{}, args ...interface{}) (*workflow.Execution, error) {
	slog.Warn("************** Using NO OP IMPL for Cadence - StartWorkflow ************** ")
	return &workflow.Execution{
		ID:    uuid.NewString() + "-NO-OP_IMPL",
		RunID: uuid.NewString() + "-NO-OP_IMPL",
	}, nil
}

func (n noOpCadenceApi) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return nil, errors.New("cannot start execute - no op cadence api implementation")
}

func (n noOpCadenceApi) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	return nil
}

func (n noOpCadenceApi) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (encoded.Value, error) {
	return nil, errors.New("cannot start query - no op cadence api implementation")
}

func (n noOpCadenceApi) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details []byte) error {
	return nil
}

func (n noOpCadenceApi) DescribeWorkflowExecution(ctx context.Context, workflowID string, runID string) (*shared.DescribeWorkflowExecutionResponse, error) {
	return nil, errors.New("no op cadence api implementation")
}
