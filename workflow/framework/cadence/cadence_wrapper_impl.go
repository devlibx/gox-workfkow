package cadence

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log/slog"
	"sync"
)

type cadenceWrapperImpl struct {
	gox.CrossFunction
	config       *Config
	workerGroups []*cadenceWorker
	zapLogger    *zap.Logger

	shoutDownOnce *sync.Once
}

func (wrapper *cadenceWrapperImpl) Start(ctx context.Context) error {
	wrapper.shoutDownOnce = &sync.Once{}

	// Make sure if context is done then we stop the = client can create and have an application
	// level context so that it can clean all resources at one
	go func() {
		<-ctx.Done()
		if ch, err := wrapper.Shutdown(ctx); err != nil {
			<-ch
		}
	}()

	if wrapper.config.Disabled {
		slog.Warn("cadence is disabled - will not start any worker")
		return nil
	}

	// The errors in the cadence logs are not very helpful. So we are disabling ing stack trace
	if !wrapper.config.EnableErrorStackInCadenceLog && wrapper.zapLogger == nil {
		var e error
		c := zap.NewProductionConfig()
		c.EncoderConfig.StacktraceKey = ""
		c.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
		if wrapper.zapLogger, e = c.Build(); e != nil {
			return errors.Wrap(e, "failed to create zap logger - stack trace is disabled in cadence log")
		}
	}

	wrapper.workerGroups = make([]*cadenceWorker, 0)
	for name, wg := range wrapper.config.WorkerGroups {
		wg.Name = name

		if wg.Disabled {
			slog.Warn("cadence worker group is disabled - worker group = %s", wg.Name)
		} else {
			worker := &cadenceWorker{
				CrossFunction:        wrapper.CrossFunction,
				createDispatcherOnce: &sync.Once{},
				workerGroup:          &wg,
				logger:               wrapper.zapLogger,
			}
			if err := worker.Start(ctx); err != nil {
				return errors.Wrap(err, "failed to start cadence worker group - worker group = %s", wg.Name)
			}
			wrapper.workerGroups = append(wrapper.workerGroups, worker)
		}
	}

	return nil
}

func (wrapper *cadenceWrapperImpl) Shutdown(ctx context.Context) (chan error, error) {
	doneCh := make(chan error, 2)
	defer func() {
		doneCh <- nil
		close(doneCh)
	}()

	wrapper.shoutDownOnce.Do(func() {
		for _, cadenceWorkerObj := range wrapper.workerGroups {
			ch := make(chan error, 2)
			if err := cadenceWorkerObj.Shutdown(ctx, ch); err == nil {
				<-ch
			}
		}
	})

	return doneCh, nil
}

func (wrapper *cadenceWrapperImpl) StartWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflowFunc interface{}, args ...interface{}) (*workflow.Execution, error) {
	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[options.TaskList]; ok {
			return cadenceWorkerObj.cadenceClient.StartWorkflow(ctx, options, workflowFunc, args...)
		}
	}
	return nil, errors.New(`task list not registered in application config to run this workflow: %s`, options.TaskList)
}

func (wrapper *cadenceWrapperImpl) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[options.TaskList]; ok {
			return cadenceWorkerObj.cadenceClient.ExecuteWorkflow(ctx, options, workflow, args...)
		}
	}
	return nil, errors.New("task list not registered in application config to run this workflow: %s", options.TaskList)
}

func (wrapper *cadenceWrapperImpl) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	taskList, err := wrapper.getTaskListFromContext(ctx)
	if err != nil {
		return err
	}

	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[taskList]; ok {
			return cadenceWorkerObj.cadenceClient.CancelWorkflow(ctx, workflowID, runID)
		}
	}
	return errors.New("task list not registered in application config to run this workflow: %s", taskList)
}

func (wrapper *cadenceWrapperImpl) QueryWorkflow(ctx context.Context, workflowID string, runID string, queryType string, args ...interface{}) (encoded.Value, error) {
	taskList, err := wrapper.getTaskListFromContext(ctx)
	if err != nil {
		return nil, err
	}

	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[taskList]; ok {
			return cadenceWorkerObj.cadenceClient.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
		}
	}
	return nil, errors.New("task list not registered in application config to run this workflow: %s", taskList)
}

func (wrapper *cadenceWrapperImpl) TerminateWorkflow(ctx context.Context, workflowID string, runID string, reason string, details []byte) error {
	taskList, err := wrapper.getTaskListFromContext(ctx)
	if err != nil {
		return err
	}

	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[taskList]; ok {
			return cadenceWorkerObj.cadenceClient.TerminateWorkflow(ctx, workflowID, runID, reason, details)
		}
	}
	return errors.New("task list not registered in application config to run this workflow: %s", taskList)
}

func (wrapper *cadenceWrapperImpl) getTaskListFromContext(ctx context.Context) (string, error) {
	if ctx.Value(TaskListForAction) == nil {
		return "", errors.New("please set task list name in context parameter - set task list name with key: %s", TaskListForAction)
	} else if _, ok := ctx.Value(TaskListForAction).(string); !ok {
		return "", errors.New("please set task list name as string in context parameter - set task list name with key: %s", TaskListForAction)
	}
	return ctx.Value(TaskListForAction).(string), nil
}
