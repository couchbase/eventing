package notifier

import (
//"testing
// "log"
// "time"
)

/*
func ExamplePoolObserver() {
	bucketChannel := make(chan testDetails, 2)
	bucketCallback := func(b *Bucket, co []*TransitionEvent, err error) {
			bucketChannel <- testDetails{
						bucket: b,
						co: co,
						err: err,
					}
		}

	poolChannel := make(chan testDetails, 2)
	poolCallback := func(co []*TransitionEvent, err error) {
                        poolChannel <- testDetails{
                                                co: co,
                                                err: err,
                                        }
                }

	responseChangeCallback := changeCallback{
		poolChangeCallback: poolCallback,
		bucketChangeCallback: bucketCallback,
	}

	_, err := newPoolObserver("default", "http://localhost:9000", responseChangeCallback)
	if err != nil {
		return
	}

	for {
		select {
		case msg := <-bucketChannel:
			log.Printf("bucketName: %s\n", msg.bucket.Name)
			for _, transition := range msg.co {
				log.Printf("Event: %v\n", transition.Event)
			}

		case msg := <-poolChannel:
			for _, transition := range msg.co {
				log.Printf("event: %v", transition.Event)
			}

		case <-time.After(20*time.Second):
			return
		}
	}
}
*/
