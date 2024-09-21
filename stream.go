/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package sse

import (
	"net/url"
	"sync"
	"sync/atomic"
)

// Stream ...
type Stream struct {
	ID              string
	event           chan *Event
	quit            chan struct{}
	quitOnce        sync.Once
	register        chan *Subscriber
	deregister      chan *Subscriber
	subscribers     map[string]*Subscriber
	Eventlog        EventLog
	subscriberCount int32
	// Enables replaying of eventlog to newly added subscribers
	AutoReplay   bool
	isAutoStream bool

	// Specifies the function to run when client subscribe or un-subscribe
	OnSubscribe   func(streamID string, sub *Subscriber)
	OnUnsubscribe func(streamID string, sub *Subscriber)
}

// newStream returns a new stream
func newStream(id string, buffSize int, replay, isAutoStream bool, onSubscribe, onUnsubscribe func(string, *Subscriber)) *Stream {
	return &Stream{
		ID:            id,
		AutoReplay:    replay,
		subscribers:   make(map[string]*Subscriber),
		isAutoStream:  isAutoStream,
		register:      make(chan *Subscriber),
		deregister:    make(chan *Subscriber),
		event:         make(chan *Event, buffSize),
		quit:          make(chan struct{}),
		Eventlog:      make(EventLog, 0),
		OnSubscribe:   onSubscribe,
		OnUnsubscribe: onUnsubscribe,
	}
}

func (str *Stream) run() {
	go func(str *Stream) {
		for {
			select {
			// Add new subscriber
			case subscriber := <-str.register:
				if oldSubscriber, ok := str.subscribers[subscriber.Id]; ok {
					str.removeSubscriber(oldSubscriber)
				}
				str.subscribers[subscriber.Id] = subscriber

				if str.AutoReplay {
					str.Eventlog.Replay(subscriber)
				}

			// Remove closed subscriber
			case subscriber := <-str.deregister:
				if _, ok := str.subscribers[subscriber.Id]; ok {
					str.removeSubscriber(subscriber)
				}

				if str.OnUnsubscribe != nil {
					go str.OnUnsubscribe(str.ID, subscriber)
				}

			// Publish event to subscribers
			case event := <-str.event:
				if str.AutoReplay {
					str.Eventlog.Add(event)
				}
				for _, subscriber := range str.subscribers {
					subscriber.connection <- event
				}

			// Shutdown if the server closes
			case <-str.quit:
				// remove connections
				str.removeAllSubscribers()
				return
			}
		}
	}(str)
}

// Send individual message
func (str *Stream) sendOne(subscriberId string, event *Event) {
	if subscriber, ok := str.subscribers[subscriberId]; ok {
		subscriber.connection <- event
	}
}

func (str *Stream) close() {
	str.quitOnce.Do(func() {
		close(str.quit)
	})
}

// addSubscriber will create a new subscriber on a stream
func (str *Stream) addSubscriber(eventid int, url *url.URL, idSubscriber string) *Subscriber {
	atomic.AddInt32(&str.subscriberCount, 1)
	sub := &Subscriber{
		Id:         idSubscriber,
		eventid:    eventid,
		quit:       str.deregister,
		connection: make(chan *Event, 64),
		URL:        url,
	}

	if str.isAutoStream {
		sub.removed = make(chan struct{}, 1)
	}

	str.register <- sub

	if str.OnSubscribe != nil {
		go str.OnSubscribe(str.ID, sub)
	}

	return sub
}

func (str *Stream) removeSubscriber(subscriber *Subscriber) {
	atomic.AddInt32(&str.subscriberCount, -1)
	close(subscriber.connection)
	if subscriber.removed != nil {
		subscriber.removed <- struct{}{}
		close(subscriber.removed)
	}
	delete(str.subscribers, subscriber.Id)
}

func (str *Stream) removeAllSubscribers() {
	for _, subscriber := range str.subscribers {
		close(subscriber.connection)
		if subscriber.removed != nil {
			subscriber.removed <- struct{}{}
			close(subscriber.removed)
		}
	}
	atomic.StoreInt32(&str.subscriberCount, 0)
	str.subscribers = map[string]*Subscriber{}
}

func (str *Stream) getSubscriberCount() int {
	return int(atomic.LoadInt32(&str.subscriberCount))
}
