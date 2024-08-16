package cadence

import (
	"context"
	"go.uber.org/fx"
)

func NewCadenceWorkflowApiInvokerAtBoot(lifecycle fx.Lifecycle, workflowApi Api) error {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return workflowApi.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			if ch, err := workflowApi.Shutdown(ctx); err == nil {
				<-ch
				return nil
			} else {
				return err
			}
		},
	})
	return nil
}
