package cadence

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	stats "github.com/devlibx/gox-metrics/v2/common"
	"github.com/devlibx/gox-metrics/v2/provider/prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"go.uber.org/zap"
	"log/slog"
	"sync"
)

const (
	cadenceClientName = "cadence-client"
	cadenceService    = "cadence-frontend"
)

type cadenceWorker struct {
	gox.CrossFunction
	workerGroup *WorkerGroup
	logger      *zap.Logger

	dispatcher           *yarpc.Dispatcher
	createDispatcherOnce *sync.Once
	cadenceServiceClient workflowserviceclient.Interface
	cadenceDomainClient  client.DomainClient
	cadenceClient        client.Client

	cadenceWorkers map[string]worker.Worker

	tallyScope tally.Scope
}

// Start starts the Cadence client
func (w *cadenceWorker) Start(ctx context.Context) error {
	var err error

	// See if w e can get the metric scope
	if pm, ok := w.CrossFunction.Metric().(*prometheus.PrometheusMetrics); ok {
		w.tallyScope = stats.TallyScopeWrapper{Scope: pm.Scope}
	} else {
		w.tallyScope = tally.NoopScope
	}

	if w.cadenceClient, err = w.buildCadenceClient(); err != nil {
		return errors.Wrap(err, "failed to build cadence client - workerGroup=%s, domain=%s", w.workerGroup.Name, w.workerGroup.Domain)
	}

	if w.cadenceDomainClient, err = w.buildDomainClient(); err != nil {
		return errors.Wrap(err, "failed to build domain client - workerGroup=%s, domain=%s", w.workerGroup.Name, w.workerGroup.Domain)
	}

	if domainInfo, err := w.cadenceDomainClient.Describe(context.Background(), w.workerGroup.Domain); err != nil {
		return errors.Wrap(err, "failed to describe domain (check if it exists) - workerGroup=%s, domain=%s", w.workerGroup.Name, w.workerGroup.Domain)
	} else {
		slog.Info("Cadence domain info", slog.String("domain", w.workerGroup.Domain), slog.Any("domainInfo", domainInfo))
	}

	w.cadenceWorkers = make(map[string]worker.Worker)

	// It's time to start the workers for each task list
	for _, taskListWorker := range w.workerGroup.Workers {
		cw := worker.New(
			w.cadenceServiceClient,
			w.workerGroup.Domain,
			taskListWorker.TaskList,
			worker.Options{
				Tracer:       opentracing.GlobalTracer(),
				MetricsScope: w.tallyScope,
				Logger:       w.logger.Named("cadence-worker-" + taskListWorker.TaskList),
			},
		)

		// Keep the worker reference - used in stopping the worker
		w.cadenceWorkers[taskListWorker.TaskList] = cw

		if err := cw.Start(); err != nil {
			return errors.Wrap(err, "failed to start worker for taskList=%s", taskListWorker.TaskList)
		}
	}

	return nil
}

// Shutdown stops the Cadence client
// This is a blocking call and done channel is used to notify when the shutdown is complete
func (w *cadenceWorker) Shutdown(ctx context.Context, doneCh chan error) error {
	defer func() {
		doneCh <- nil
		close(doneCh)
	}()

	for taskList, cadenceWorkerObj := range w.cadenceWorkers {
		cadenceWorkerObj.Stop()
		slog.Info("cadence worker stopped...", slog.String("taskList", taskList))
	}

	return nil
}

func (w *cadenceWorker) buildCadenceClient() (client.Client, error) {
	if service, err := w.buildCadenceServiceClient(); err == nil {
		return client.NewClient(service, w.workerGroup.Domain, &client.Options{
			MetricsScope: w.tallyScope,
		}), nil
	} else {
		return nil, err
	}
}

func (w *cadenceWorker) buildCadenceServiceClient() (workflowserviceclient.Interface, error) {
	if dispatcher, err := w.buildAndStartDispatcher(); err == nil {
		w.cadenceServiceClient = workflowserviceclient.New(dispatcher.ClientConfig(cadenceService))
		return w.cadenceServiceClient, nil
	} else {
		return nil, errors.Wrap(err, "failed to build and start dispatcher")
	}
}

func (w *cadenceWorker) buildDomainClient() (client.DomainClient, error) {
	w.cadenceDomainClient = client.NewDomainClient(w.cadenceServiceClient, &client.Options{
		MetricsScope: w.tallyScope,
	})
	return w.cadenceDomainClient, nil
}

// NewWorker creates a new dispatcher and also starts it
func (w *cadenceWorker) buildAndStartDispatcher() (dispatcher *yarpc.Dispatcher, err error) {
	w.createDispatcherOnce.Do(func() {
		w.dispatcher, err = w.implBuildAndStartDispatcher()
	})
	return w.dispatcher, err
}

// implBuildAndStartDispatcher creates a new dispatcher and also starts it
func (w *cadenceWorker) implBuildAndStartDispatcher() (*yarpc.Dispatcher, error) {
	serviceName := cadenceClientName + "_" + w.workerGroup.Name

	channelTransport, err := tchannel.NewChannelTransport(tchannel.ServiceName(serviceName))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel transport for cadenceClient=%s", serviceName)
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: serviceName,
		Outbounds: yarpc.Outbounds{
			cadenceService: {Unary: channelTransport.NewSingleOutbound(w.workerGroup.HostPort)},
		},
	})

	if err = dispatcher.Start(); err != nil {
		return nil, errors.Wrap(err, "field to start dispatcher for cadenceClient=%s", serviceName)
	}

	return dispatcher, nil
}
