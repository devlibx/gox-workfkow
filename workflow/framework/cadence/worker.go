package cadence

import (
	"context"
	"github.com/devlibx/gox-base"
	"github.com/devlibx/gox-base/errors"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/worker"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
	"sync"
)

const (
	cadenceClientName = "cadence-client"
	cadenceService    = "cadence-frontend"
)

type cadenceWorker struct {
	gox.CrossFunction
	workerGroup *WorkerGroup

	dispatcher           *yarpc.Dispatcher
	createDispatcherOnce *sync.Once
	cadenceServiceClient workflowserviceclient.Interface
	cadenceClient        client.Client

	cadenceWorkers map[string]worker.Worker
}

// Start starts the Cadence client
func (w *cadenceWorker) Start(ctx context.Context) error {
	var err error

	if w.cadenceClient, err = w.buildCadenceClient(); err != nil {
		return errors.Wrap(err, "failed to build cadence client - workerGroup=%s, domain=%s", w.workerGroup.Name, w.workerGroup.Domain)
	}

	w.cadenceWorkers = make(map[string]worker.Worker)

	// It's time to start the workers for each task list
	for _, taskListWorker := range w.workerGroup.Workers {
		cw := worker.New(
			w.cadenceServiceClient,
			w.workerGroup.Domain,
			taskListWorker.TaskList,
			worker.Options{
				Tracer: opentracing.GlobalTracer(),
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

	return nil
}

func (w *cadenceWorker) buildCadenceClient() (client.Client, error) {
	if service, err := w.buildCadenceServiceClient(); err == nil {
		return client.NewClient(service, w.workerGroup.Domain, &client.Options{}), nil
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
