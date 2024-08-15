package cadence

import (
	"context"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"go.uber.org/cadence/client"
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
}

func (wrapper *cadenceWrapperImpl) Start(ctx context.Context, config *Config) error {
	if config.Disabled {
		slog.Warn("cadence is disabled - will not start any worker")
		return nil
	}

	// The errors in the cadence logs are not very helpful. So we are disabling ing stack trace
	if !config.EnableErrorStackInCadenceLog && wrapper.zapLogger == nil {
		var e error
		c := zap.NewProductionConfig()
		c.EncoderConfig.StacktraceKey = ""
		c.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
		if wrapper.zapLogger, e = c.Build(); e != nil {
			return errors.Wrap(e, "failed to create zap logger - stack trace is disabled in cadence log")
		}
	}

	wrapper.workerGroups = make([]*cadenceWorker, 0)
	for name, wg := range config.WorkerGroups {
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

	for _, cadenceWorkerObj := range wrapper.workerGroups {
		ch := make(chan error, 2)
		if err := cadenceWorkerObj.Shutdown(ctx, ch); err == nil {
			<-ch
		}
	}

	return doneCh, nil
}

func (wrapper *cadenceWrapperImpl) StartWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflowFunc interface{}, args ...interface{}) (*workflow.Execution, error) {
	for _, cadenceWorkerObj := range wrapper.workerGroups {
		if _, ok := cadenceWorkerObj.cadenceWorkers[options.TaskList]; ok {
			return cadenceWorkerObj.cadenceClient.StartWorkflow(ctx, options, workflowFunc, args...)
		}
	}
	return nil, errors.New("task list not registered in application config to run this workflow: %s", options.TaskList)
}

func (wrapper *cadenceWrapperImpl) CancelWorkflow(ctx context.Context, workflowID string, runID string) error {
	return errors.New("not implemented")
}
