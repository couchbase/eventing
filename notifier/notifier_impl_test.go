package notifier

import (
	"testing"
	"time"
)

// Test for subscriberSendMedium
func TestSendFail(t *testing.T) {
	event := &TransitionEvent{}

	// unbuffered channel
	ssm := newSubscriberSendMedium(0)
	defer ssm.close()

	answerChan := make(chan bool)
	go func() {
		sent := ssm.send(event)
		answerChan <- sent
	}()

	select {
	case <-ssm.sendChannel:
	case <-time.After(10 * time.Second):
		t.Fatalf("Expected send request in 10 seconds")
	}

	sent := <-answerChan
	if !sent {
		t.Fatalf("Expected sent to be success")
	}

	// test failed send
	sent = ssm.send(event)
	if sent {
		t.Fatalf("Expected failed sent got success")
	}

	// send it again for closed sendMedium
	sent = ssm.send(event)
	if sent {
		t.Fatalf("Expected failed sent got success")
	}

	testEvent := <-ssm.receive()
	if testEvent != nil {
		t.Fatalf("Expected closed channel: %v", testEvent)
	}

	if !ssm.closed {
		t.Fatalf("Expected closed variable to be true")
	}
}

func TestSubscriberList(t *testing.T) {
	transEvent := &TransitionEvent{
		Event: InterestedEvent{
			Event:  EventKVTopologyChanges,
			Filter: "abc",
		},
		CurrentState: 2,
	}

	id := 1
	subList := newSubscriberList(transEvent.Event, id)

	result := subList.getOrAddSubscriber(nil)
	if val, ok := result.(int); !ok || val != 1 {
		t.Fatalf("getOrAdd function returned incorrect result: %v expected %d", result, id)
	}

	subscribers := []*subscriber{
		newSubscriber(1, defaultSendMediumCapacity),
		newSubscriber(2, defaultSendMediumCapacity),
		newSubscriber(3, defaultSendMediumCapacity),
	}

	for _, s := range subscribers {
		if val, ok := subList.getOrAddSubscriber(s).(int); !ok || val != 1 {
			t.Fatalf("Incorrect result returned %d converted: %v", val, ok)
		}
	}

	subList.send(transEvent)
	for _, s := range subscribers {
		select {
		case event := <-s.WaitForEvent():
			if event.CurrentState.(int) != 2 {
				t.Fatalf("Incorrect current state after setting different value")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No value after 5 seconds: %v", s)
		}
	}

	subList.remove(subscribers[0])
	transEvent.CurrentState = 3
	subList.send(transEvent)
	for _, s := range subscribers[1:] {
		select {
		case event := <-s.WaitForEvent():
			if event.CurrentState.(int) != 3 {
				t.Fatalf("Incorrect current state after setting different value")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No value after 5 seconds: %v", s)
		}
	}

	subList.close()
	for _, s := range subscribers[1:] {
		select {
		case event := <-s.WaitForEvent():
			if event.Event != transEvent.Event {
				t.Fatalf("Incorrect event called")
			}

			if !event.Deleted {
				t.Fatalf("Incorrect current state after setting different value")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No value after 5 seconds: %v", s)
		}
	}

	if len(subList.subscriberChannel) != 0 {
		t.Fatalf("Subscriber list not empty after closing the sublist")
	}
}

func TestBucketChangeCallback(t *testing.T) {
	ob := newObserver()
	sub := newSubscriber(1, defaultSendMediumCapacity)

	iE := InterestedEvent{
		Filter: "testBucket",
	}

	b := &Bucket{
		Name: "testBucket",
		UUID: "ABC",
	}
	// Create bucket observer
	ob.BucketChangeCallback(b, nil, nil)

	iE.Event = EventBucketChanges
	ob.getOrAddSubscriber(iE, sub)
	iE.Event = EventVbmapChanges
	ob.getOrAddSubscriber(iE, sub)
	iE.Event = EventScopeOrCollectionChanges
	ob.getOrAddSubscriber(iE, sub)

	syncChan := make(chan bool)
	go func() {
		event := []*TransitionEvent{
			&TransitionEvent{
				Event:        iE,
				CurrentState: 1,
			},
		}
		ob.BucketChangeCallback(b, event, nil)
		<-syncChan

		iE.Event = EventVbmapChanges
		event[0].CurrentState = 2
		event[0].Event = iE
		ob.BucketChangeCallback(b, event, nil)
		<-syncChan

		ob.BucketChangeCallback(b, nil, ErrBucketDeleted)
	}()

	event := <-sub.WaitForEvent()
	if val, ok := event.CurrentState.(int); !ok || val != 1 {
		t.Fatalf("Wrong current state")
	}
	syncChan <- true

	event = <-sub.WaitForEvent()
	if val, ok := event.CurrentState.(int); !ok || val != 2 {
		t.Fatalf("Wrong current state expected 2")
	}
	syncChan <- true

	for i := 0; i < 3; i++ {
		select {
		case event = <-sub.WaitForEvent():
			if !event.Deleted {
				t.Fatalf("Expected deleted filter got non delete filter")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Not got any response in 5 seconds")
		}
	}
}

func BenchmarkBucketChange(b *testing.B) {
	ob := newObserver()

	bucket := &Bucket{
		Name: "testBucket",
		UUID: "ABC",
	}

	iE := InterestedEvent{
		Event:  EventVbmapChanges,
		Filter: "testBucket",
	}

	event := []*TransitionEvent{
		&TransitionEvent{
			Event:        iE,
			CurrentState: 1,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.BucketChangeCallback(bucket, event, nil)
	}
}
