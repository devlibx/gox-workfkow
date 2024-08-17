package cadence

import (
	"context"
	"go.uber.org/fx"
)

func NewCadenceWorkflowApiInvokerAtBoot(lifecycle fx.Lifecycle, workflowApi Api, config *Config) error {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if config.Disabled {
				return nil
			}
			return workflowApi.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			if config.Disabled || workflowApi == nil {
				return nil
			}
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
