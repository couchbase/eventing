package notification

import (
	"errors"
	"time"
)

// Below lists all errors the API can return in expected scenarios. WIP.
type Code error

var (
	TopicNotFound    Code = errors.New("topic does not exist")
	TopicSliceClosed Code = errors.New("topic slice is closed")
	Timeout          Code = errors.New("operation timed out")
)

type Error interface {
	// The returned error code is stable and comparable
	Code() Code

	// Details field contains additional information. Should not be parsed or compared.
	Details() string
}

type Notification interface {
	// Key is a opaque identifier, and is unique for a given TopicSlice.
	// Reading the key of an expired notification could return a Timeout error.
	Key() (string, Error)

	// Value stores the actual notification content. Substructure may specify a type.
	// Reading the value of an expired notification could return a Timeout error.
	Value() (interface{}, Error)

	// Lease expiration time of the notification. After this, the notification may
	// have been re-queued. It is allowable to call Expiry() on expired notifications.
	Expiry() time.Time
}

type TopicSlice interface {
	// Get the channel from which notifications can be read. The returned object is valid
	// until it is acknowledged or lease expires. Caller should never close this channel.
	NotifyChannel() (<-chan Notification, Error)

	// Acknowledge the completion of processing of a notification. Notifications must be returned
	// to this channel after completion of their processing and prior to their lease expiry.
	// Once queued on this channel, the underlying object is no longer valid for further use.
	// Caller should never close this channel.
	AckChannel() (chan<- Notification, Error)

	// Request that no more items should be queued into NotificationChannel. Any notifications
	// already queued on the channel are valid and must be processed before expiry.
	Pause() Error

	// Reverse the effect of a previous Pause() call.
	Resume() Error

	// Describe the definition time characteristics of this topic
	Describe() (*TopicDef, Error)

	// Close a TopicSlice. After this is called, no items must be read from notification channel, and
	// no items must be queued to acknowledgement channel, and notification objects held are invalid.
	// A topic should ideally not be closed when there are unread or unacknowledged items, but if done,
	// unread and unacknowledged items will be re-queued at unspecified time and in unspecified order.
	Close() Error
}

type Characteristic int

const (
	UnreliableDelivery Characteristic = iota
	ReliableDelivery

	Unordered
	PartiallyOrdered

	DropsAtHeadWhenFull
	PausesInsertWhenFull
)

// This API returns only the following combinations
var (
	Ephemeral  = &[]Characteristic{DropsAtHeadWhenFull, PartiallyOrdered, UnreliableDelivery}
	Persistent = &[]Characteristic{PartiallyOrdered, PausesInsertWhenFull, ReliableDelivery}
)

type TopicDef interface {
	// Characteristics of notifications. Can pointer compare to the declared combinations above.
	QoS() (*[]Characteristic, Error)

	// The number of notifications the topic will hold before becoming "Full"
	// Returns math.MaxInt64 is the topic is unbounded.
	Capacity() (uint64, Error)

	// The lease duration of notifications sent via the API
	Lease() (time.Duration, Error)

	// Check if a user can access this topic
	CheckUser(user, pass string) (authenticated, authorized bool, err Error)
}

type NotificationManager interface {
	// Open a TopicSlice. In order to see all notifications on a topic, a TopicSlice must be opened
	// on each eventing node. It is the responsibility of ns_server to identify the list of such nodes.
	// On a given node, for a given topic, exactly one TopicSlice must be opened. Calling Open more than once
	// for a given topic on a given node will cause all previously opened TopicSlices for the topic on this node
	// to be deemed as implcitly Close()-ed but timing of such implicit closure is unspecified.
	OpenTopic(topic string) (*TopicSlice, Error)

	// Describe the definition time characteristics of a topic. This is a superset of what appears in the UI.
	DescribeTopic(topic string) (*TopicDef, Error)
}

// Singleton object set by implementation. Caller should use this directly (do not copy).
var Manager NotificationManager
