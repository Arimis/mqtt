package mqtt

import (
	"errors"
	"strconv"
	"time"

	"github.com/casaplatform/casa"
	"github.com/gomqtt/client"
	"github.com/gomqtt/packet"
)

type Client struct {
	timeout time.Duration

	client *client.Client

	options  *client.Config
	session  client.Session
	callback client.Callback
	logger   client.Logger

	userCallback func(*casa.Message, error)
}

func NewClient(url string, options ...ClientOption) (*Client, error) {
	newClient := &Client{
		options: client.NewConfig(url),
		timeout: 1 * time.Second,
	}

	for _, option := range options {
		option(newClient)
	}

	newClient.client = client.New()

	newClient.client.Logger = newClient.logger

	connectFuture, err := newClient.client.Connect(newClient.options)
	if err != nil {
		return nil, err
	}

	err = connectFuture.Wait(newClient.timeout)
	if err != nil {
		return nil, err
	}

	newClient.client.Callback = func(msg *packet.Message, err error) {
		if newClient.userCallback != nil {
			var m *casa.Message
			if err == nil {
				m = &casa.Message{
					msg.Topic, msg.Payload,
					msg.Retain,
				}
			}

			go newClient.userCallback(m, err)
		}
	}

	return newClient, nil
}

func (c *Client) Handle(f func(msg *casa.Message, err error)) {
	c.userCallback = f
}

func (c *Client) Subscribe(topic string) error {
	if !subValid(topic) {
		return ErrInvalidSub
	}

	subscribeFuture, err := c.client.Subscribe(topic, packet.QOSExactlyOnce)
	if err != nil {
		return err
	}

	err = subscribeFuture.Wait(c.timeout)
	if err != nil {
		return err
	}
	return nil

}

func (c *Client) Unsubscribe(topic string) error {
	if !subValid(topic) {
		return ErrInvalidSub
	}

	unsubscribeFuture, err := c.client.Unsubscribe(topic)
	if err != nil {
		return err
	}

	err = unsubscribeFuture.Wait(c.timeout)
	if err != nil {
		return err
	}
	return nil

}

func (c *Client) PublishMessage(message casa.Message) error {
	if message.Topic == "" {
		return errors.New("Message topic cannot be empty")
	}

	if !topicValid(message.Topic) {
		return ErrInvalidTopic
	}

	publishFuture, err := c.client.PublishMessage(
		&packet.Message{
			Topic:   message.Topic,
			Payload: message.Payload,
			Retain:  message.Retain,
			QOS:     packet.QOSExactlyOnce,
		})
	if err != nil {
		return err
	}

	err = publishFuture.Wait(c.timeout)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) Close() error {
	return c.client.Disconnect(c.timeout)
}

type ClientOption func(*Client) error

func ClientID(id string) ClientOption {
	return func(c *Client) error {
		c.options.ClientID = id
		return nil
	}
}

func CleanSession(clean bool) ClientOption {
	return func(c *Client) error {
		c.options.CleanSession = clean
		return nil
	}
}

func KeepAlive(duration time.Duration) ClientOption {
	return func(c *Client) error {
		seconds := strconv.FormatFloat(duration.Seconds(), 'f', 0, 64)
		c.options.KeepAlive = seconds
		return nil
	}
}

func Will(message *casa.Message) ClientOption {
	return func(c *Client) error {
		c.options.WillMessage = &packet.Message{
			Topic:   message.Topic,
			Payload: message.Payload,
			QOS:     packet.QOSExactlyOnce,
			Retain:  message.Retain,
		}
		return nil
	}
}

func ValidateSubs(validate bool) ClientOption {
	return func(c *Client) error {
		c.options.ValidateSubs = validate
		return nil
	}
}

func Timeout(timeout time.Duration) ClientOption {
	return func(c *Client) error {
		c.timeout = timeout
		return nil
	}
}

func ClientLogger(logger client.Logger) ClientOption {
	return func(c *Client) error {
		c.logger = logger
		return nil
	}
}
