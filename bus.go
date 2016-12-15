package mqtt

import (
	"errors"
	"log"

	"crypto/tls"

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
	users      map[string]string

	tls          bool
	certificates []tls.Certificate

	launcher   transport.Launcher
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

	backend := broker.NewMemoryBackend()

	if len(bus.users) > 0 {
		backend.Logins = bus.users
	}

	bus.engine = broker.NewEngineWithBackend(backend)

	bus.engine.Logger = bus.logger
	if len(bus.transportList) < 1 {
		return nil, ErrNoURL
	}

	bus.launcher = transport.Launcher{
		TLSConfig: &tls.Config{},
	}

	if bus.tls {
		bus.launcher.TLSConfig.Certificates = bus.certificates
	}

	for _, url := range bus.transportList {
		server, err := bus.launcher.Launch(url)
		if err != nil {
			return nil, err
		}

		log.Println("Listening on:", url)
		bus.engine.Accept(server)
		bus.transports = append(bus.transports, server)
	}

	return bus, nil
}

func Users(users map[string]string) BusOption {
	return func(b *Bus) error {
		b.users = users
		return nil
	}
}

func TLS(cert tls.Certificate) BusOption {
	return func(b *Bus) error {
		b.tls = true
		b.certificates = append(b.certificates, cert)
		return nil
	}
}
func ListenOn(urls ...string) BusOption {
	return func(b *Bus) error {
		if b.transportList == nil {
			b.transportList = make([]string, 1)
		}

		for _, url := range urls {
			b.transportList = append(b.transportList, url)
		}

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
