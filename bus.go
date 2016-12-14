package mqtt

import (
	"errors"
	"log"

	"github.com/casaplatform/casa"
	"github.com/gomqtt/broker"
	"github.com/gomqtt/transport"
)

var (
	ErrNoURL = errors.New("No URL specified to listen on")
)

type Bus struct {
	transportList []string

	transports []transport.Server
	backend    broker.Backend
	logger     broker.Logger
	engine     *broker.Engine

	clientsURL string
}

func (b *Bus) NewClient() casa.MessageClient {
	client, err := NewClient(b.clientsURL)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

var DefaultBackend = broker.NewMemoryBackend()

type BusOption func(*Bus) error

// Initialize a new Bus
func New(options ...BusOption) (casa.MessageBus, error) {
	bus := &Bus{
		transportList: make([]string, 0),
	}

	for _, option := range options {
		option(bus)
	}

	if bus.engine == nil {
		bus.engine = broker.NewEngineWithBackend(DefaultBackend)
	}
	bus.engine.Logger = bus.logger
	if len(bus.transportList) < 1 {
		return nil, ErrNoURL
	}

	for _, url := range bus.transportList {
		server, err := transport.Launch(url)
		if err != nil {
			return nil, err
		}
		bus.engine.Accept(server)
		bus.transports = append(bus.transports, server)
	}

	return bus, nil
}

func ListenOn(url string) BusOption {
	return func(b *Bus) error {
		if b.transportList == nil {
			b.transportList = make([]string, 1)
		}
		b.transportList = append(b.transportList, url)

		return nil
	}
}

func Backend(backend broker.Backend) BusOption {
	return func(b *Bus) error {
		b.engine = broker.NewEngineWithBackend(backend)
		return nil
	}
}

func BrokerLogger(logger broker.Logger) BusOption {
	return func(b *Bus) error {
		b.logger = logger
		return nil
	}
}

func (b *Bus) Close() error {
	for _, t := range b.transports {
		err := t.Close()
		if err != nil {
			return err
		}
	}
	b.engine.Close()
	return nil
}
