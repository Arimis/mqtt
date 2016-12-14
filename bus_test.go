package mqtt

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/casaplatform/casa"
)

func TestSatisfies_Bus(t *testing.T) {
	b := &Bus{}
	func(b casa.MessageBus) {}(b)
}

func TestSatisfies_Client(t *testing.T) {
	b := &Client{}
	func(b casa.MessageClient) {}(b)
}

func handler(msgChan chan *casa.Message, errChan chan error) func(*casa.Message, error) {
	return func(msg *casa.Message, err error) {
		if err != nil {
			errChan <- err
		}
		msgChan <- msg
	}
}

// Big ugly test
func TestEventBus(t *testing.T) {
	bus, err := New(ListenOn("tcp://:1883"))
	if err != nil {
		t.Fatal(err)
	}

	msgChan1 := make(chan *casa.Message)
	errChan1 := make(chan error)

	handler1 := handler(msgChan1, errChan1)

	msgChan2 := make(chan *casa.Message)
	errChan2 := make(chan error)

	handler2 := handler(msgChan2, errChan2)

	clientAll, err := NewClient("tcp://:1883", ClientID("test all client"))
	if err != nil {
		t.Fatal(err)
	}

	clientAll.Handle(handler1)

	err = clientAll.Subscribe("#")
	if err != nil {
		t.Fatal(err)
	}

	clientSome, err := NewClient("tcp://:1883", ClientID("test some client"))
	if err != nil {
		t.Fatal(err)
	}

	clientSome.Handle(handler2)

	err = clientSome.Subscribe("test/+")
	if err != nil {
		t.Fatal(err)
	}

	err = clientAll.PublishMessage(casa.Message{
		Topic: "test"})
	if err != nil {
		t.Fatal(err)
	}
	timeout := 10 * time.Millisecond

	allMsg, timedOut := timeoutRead(msgChan1, timeout)
	if timedOut {
		t.Fail()
	}

	if allMsg.Topic != "test" {
		t.Fail()
	}

	someMsg, timedOut := timeoutRead(msgChan2, timeout)
	if !timedOut {
		t.Fail()
	}

	if someMsg != nil {
		t.Fail()
	}

	err = clientSome.PublishMessage(casa.Message{
		Topic:   "test/test",
		Payload: []byte("payload")})
	if err != nil {
		t.Fatal(err)
	}
	allMsg, timedOut = timeoutRead(msgChan1, timeout)
	if timedOut {
		t.Fail()
	}

	if allMsg.Topic != "test/test" {
		t.Fail()
	}

	if string(allMsg.Payload) != "payload" {
		t.Fail()
	}

	someMsg, timedOut = timeoutRead(msgChan2, timeout)
	if timedOut {
		t.Fail()
	}

	if allMsg.Topic != "test/test" {
		t.Fail()
	}

	if string(allMsg.Payload) != "payload" {
		t.Fail()
	}

	err = clientAll.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = clientSome.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = bus.Close()
	if err != nil {
		t.Fatal(err)
	}

}

/*
func TestBus_Unsubscribe(t *testing.T) {

	bus, err := New()
	if err != nil {
		t.Fatal(err)
	}
	sub1, err := bus.Subscribe("#")
	if err != nil {
		t.Fatal(err)
	}
	sub2, err := bus.Subscribe("test/#")
	if err != nil {
		t.Fatal(err)
	}

	if len(bus.subs) != 2 {
		t.Fail()
	}

	bus.Unsubscribe(sub1)
	if len(bus.subs) != 1 {
		t.Fail()
	}

	bus.Unsubscribe(sub2)
	if len(bus.subs) != 0 {
		t.Fail()
	}
	err = bus.Stop()
	if err != nil {
		t.Fatal(err)
	}

}
*/
func timeoutRead(c chan *casa.Message, d time.Duration) (*casa.Message, bool) {
	select {
	case m := <-c:
		return m, false
	case <-time.After(d):
		return nil, true
	}
}

type check struct {
	Sub, Topic  string
	ShouldMatch bool
	ValidTopic  bool
	ValidSub    bool
}

var tests = []check{
	// Lots of tests from lots of examples. This can certainly be cleaned
	// up and simplified... later...
	{"myhome/groundfloor/+/temperature", "myhome/groundfloor/livingroom/temperature", true, true, true},
	{"myhome/groundfloor/+/temperature", "myhome/groundfloor/kitchen/temperature", true, true, true},
	{"myhome/groundfloor/+/temperature", "myhome/groundfloor/kitchen/brightness", false, true, true},
	{"myhome/groundfloor/+/temperature", "myhome/firstfloor/kitchen/temperature", false, true, true},
	{"myhome/groundfloor/+/temperature", "myhome/groundfloor/kitchen/fridge/temperature", false, true, true},

	{"myhome/groundfloor/#", "myhome/groundfloor/livingroom/temperature", true, true, true},
	{"myhome/groundfloor/#", "myhome/groundfloor/kitchen/temperature", true, true, true},
	{"myhome/groundfloor/#", "myhome/groundfloor/kitchen/brightness", true, true, true},
	{"myhome/groundfloor/#", "myhome/firstfloor/kitchen/temperature", false, true, true},

	{"a/b/c/d", "a/b/c/d", true, true, true},
	{"+/b/c/d", "a/b/c/d", true, true, true},
	{"a/+/c/d", "a/b/c/d", true, true, true},
	{"a/+/+/d", "a/b/c/d", true, true, true},
	{"+/+/+/+", "a/b/c/d", true, true, true},

	{"a/b/c", "a/b/c/d", false, true, true},
	{"b/+/c/d", "a/b/c/d", false, true, true},
	{"+/+/+", "a/b/c/d", false, true, true},

	{"#", "a/b/c/d", true, true, true},
	{"a/#", "a/b/c/d", true, true, true},
	{"a/b/#", "a/b/c/d", true, true, true},
	{"a/b/c/#", "a/b/c/d", true, true, true},
	{"+/b/c/#", "a/b/c/d", true, true, true},

	{"a/+/topic", "a//topic", true, true, true},
	{"+/a/topic", "/a/topic", true, true, true},
	{"#", "/a/topic", true, true, true},
	{"a/topic/+", "a/topic/", true, true, true},

	{"a/topic/#", "a/topic/", true, true, true},

	{"sport/tennis/player1/#", "sport/tennis/player1", true, true, true},
	{"sport/tennis/player1/#", "sport/tennis/player1/ranking", true, true, true},
	{"sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", true, true, true},

	{"sport/#", "sport/tennis/player1", true, true, true},
	{"sport/tennis/#", "sport/tennis/player1", true, true, true},
	{"sport/tennis#", "sport/tennis/player1", false, true, false},
	{"sport/tennis/#/ranking", "sport/tennis/player1", false, true, false},

	{"sport/tennis/+", "sport/tennis/player1", true, true, true},
	{"sport/tennis/+", "sport/tennis/player1/ranking", false, true, true},
	{"sport/+", "sport", false, true, true},
	{"sport/+", "sport/", true, true, true},

	{"+/+", "/finance", true, true, true},
	{"/+", "/finance", true, true, true},
	{"+", "/finance", false, true, true},

	{"foo/#/bar", "foo/#", false, false, false},
	{"foo/+bar", "foo+", false, false, false},
	{"foo/bar#", "foo/+", false, false, false},
}

func TestTopicMatch(t *testing.T) {
	for i, v := range tests {
		if topicMatch(v.Sub, v.Topic) != v.ShouldMatch {
			t.Fatalf("Failed on %v: %v", i, v)
		}
	}

}

func TestTopicValid(t *testing.T) {
	for i, v := range tests {
		if topicValid(v.Topic) != v.ValidTopic {
			t.Fatalf("Failed on %v: %v", i, v)
		}
	}

}
func TestSubValid(t *testing.T) {
	for i, v := range tests {
		if subValid(v.Sub) != v.ValidSub {
			t.Fatalf("Failed on %v: %v", i, v)
		}
	}

}

func TestLongSub(t *testing.T) {
	if subValid(strings.Repeat("g", 65536)) {
		t.Fatalf("Long sub failed")
	}
	if !subValid(strings.Repeat("g", 65535)) {
		t.Fatalf("Longest sub failed")
	}
}

func TestLongTopic(t *testing.T) {
	if topicValid(strings.Repeat("g", 65536)) {
		t.Fatalf("Long topic failed")
	}
	if !topicValid(strings.Repeat("g", 65535)) {
		t.Fatalf("Longest topic failed")
	}
}

func messageEqual(e1, e2 *casa.Message) bool {
	if e1.Topic != e2.Topic {
		return false
	}
	if !reflect.DeepEqual(e1.Payload, e2.Payload) {
		return false
	}
	return true
}
