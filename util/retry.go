package util

import (
	"math/rand"
	"time"

	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/logging"
)

const (
	Stop time.Duration = -1

	DefaultInitialInterval     = 1 * time.Second
	DefaultRandomizationFactor = 0.5
	DefaultMultiplier          = 1.5
	DefaultMaxInterval         = 10 * time.Second
	DefaultMaxElapsedTime      = time.Minute
)

type Backoff interface {
	NextBackoff() time.Duration
	Reset()
}

type FixedBackoff struct {
	Interval time.Duration
}

type ExponentialBackoff struct {
	InitialInterval     time.Duration
	RandomizationFactor float64
	Multiplier          float64
	MaxInterval         time.Duration

	// After MaxElapsedTime the ExponentialBackoff stops.
	// It never stops if MaxElapsedTime == 0.
	MaxElapsedTime time.Duration
	Clock          Clock

	currentInterval time.Duration
	startTime       time.Time
}

func NewFixedBackoff(d time.Duration) *FixedBackoff {
	return &FixedBackoff{
		Interval: d,
	}
}

func (b *FixedBackoff) NextBackoff() time.Duration {
	return b.Interval
}

func (b *FixedBackoff) Reset() {
}

type CallbackFunc func(arg ...interface{}) error

func Retry(b Backoff, retryCount *int64, callback CallbackFunc, args ...interface{}) error {
	var next time.Duration
	retries := int64(0)

	for {
		err := callback(args...)

		if err == nil {
			return nil
		}

		if (err == common.ErrRetryTimeout) ||
			(retryCount != nil && *retryCount != -1 && retries >= *retryCount) {
			return common.ErrRetryTimeout
		}

		if next = b.NextBackoff(); next == Stop {
			return err
		}

		logging.Debugf("RTLP Retrying after %vs", next.Seconds())
		time.Sleep(next)
		retries++
	}
}

type Clock interface {
	Now() time.Time
}

func NewExponentialBackoff() *ExponentialBackoff {
	b := &ExponentialBackoff{
		InitialInterval:     DefaultInitialInterval,
		RandomizationFactor: DefaultRandomizationFactor,
		Multiplier:          DefaultMultiplier,
		MaxInterval:         DefaultMaxInterval,
		MaxElapsedTime:      DefaultMaxElapsedTime,
		Clock:               SystemClock,
	}
	if b.RandomizationFactor < 0 {
		b.RandomizationFactor = 0
	} else if b.RandomizationFactor > 1 {
		b.RandomizationFactor = 1
	}
	b.Reset()
	return b
}

type systemClock struct{}

func (t systemClock) Now() time.Time {
	return time.Now()
}

var SystemClock = systemClock{}

func (b *ExponentialBackoff) Reset() {
	b.currentInterval = b.InitialInterval
	b.startTime = b.Clock.Now()
}

func (b *ExponentialBackoff) NextBackoff() time.Duration {
	if b.MaxElapsedTime != 0 && b.GetElapsedTime() > b.MaxElapsedTime {
		return Stop
	}
	defer b.incrementCurrentInterval()
	return getRandomValueFromInterval(b.RandomizationFactor, rand.Float64(), b.currentInterval)
}

func (b *ExponentialBackoff) GetElapsedTime() time.Duration {
	return b.Clock.Now().Sub(b.startTime)
}

func (b *ExponentialBackoff) incrementCurrentInterval() {
	if float64(b.currentInterval) >= float64(b.MaxInterval)/b.Multiplier {
		b.currentInterval = b.MaxInterval
	} else {
		b.currentInterval = time.Duration(float64(b.currentInterval) * b.Multiplier)
	}
}

func getRandomValueFromInterval(randomizationFactor, random float64, currentInterval time.Duration) time.Duration {
	var delta = randomizationFactor * float64(currentInterval)
	var minInterval = float64(currentInterval) - delta
	var maxInterval = float64(currentInterval) + delta

	return time.Duration(minInterval + (random * (maxInterval - minInterval + 1)))
}
