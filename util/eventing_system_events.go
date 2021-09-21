package util

import (
	"errors"

	"github.com/couchbase/goutils/systemeventlog"
	"github.com/couchbase/eventing/logging"
)

const (
	SYSTEM_EVENT_COMPONENT = "eventing"

	SUB_COMPONENT_EVENTING_PRODUCER = "eventing-producer"

	DEFAULT_TIMEOUT_SECS = 2
)

const (
	// Eventing's EventId range: 4096-5119

	EVENTID_PRODUCER_STARTUP = 4096

	EVENTID_CONSUMER_STARTUP = 4097
	EVENTID_CONSUMER_CRASH = 4098

	EVENTID_START_TRACING = 4099
	EVENTID_STOP_TRACING  = 4100

	EVENTID_START_DEBUGGER = 4101
	EVENTID_STOP_DEBUGGER  = 4102

	EVENTID_CREATE_FUNCTION  = 4103
	EVENTID_DELETE_FUNCTION  = 4104
	EVENTID_IMPORT_FUNCTIONS = 4105
	EVENTID_EXPORT_FUNCTIONS = 4106

	last_EVENTID_FOR_EVENTING = 5119 // Must not go beyond this.
)

var (
	eventingSystemEvents = map[systemeventlog.EventId]systemeventlog.SystemEventInfo{
		//
		EVENTID_PRODUCER_STARTUP: systemeventlog.SystemEventInfo{EVENTID_PRODUCER_STARTUP, "eventing-producer process startup"},
		//
		EVENTID_CONSUMER_STARTUP: systemeventlog.SystemEventInfo{EVENTID_CONSUMER_STARTUP, "eventing-consumer process startup"},
		EVENTID_CONSUMER_CRASH: systemeventlog.SystemEventInfo{EVENTID_CONSUMER_CRASH, "eventing-consumer process crash"},
		//
		EVENTID_START_TRACING: systemeventlog.SystemEventInfo{EVENTID_START_TRACING, "Tracing started"},
		EVENTID_STOP_TRACING:  systemeventlog.SystemEventInfo{EVENTID_STOP_TRACING, "Tracing stopped"},
		//
		EVENTID_START_DEBUGGER: systemeventlog.SystemEventInfo{EVENTID_START_DEBUGGER, "Debugger started"},
		EVENTID_STOP_DEBUGGER:  systemeventlog.SystemEventInfo{EVENTID_STOP_DEBUGGER, "Debugger stopped"},
		//
		EVENTID_CREATE_FUNCTION: systemeventlog.SystemEventInfo{EVENTID_CREATE_FUNCTION, "Create Function"},
		EVENTID_DELETE_FUNCTION: systemeventlog.SystemEventInfo{EVENTID_DELETE_FUNCTION, "Delete Function"},
		//
		EVENTID_IMPORT_FUNCTIONS: systemeventlog.SystemEventInfo{EVENTID_IMPORT_FUNCTIONS, "Import Functions"},
		EVENTID_EXPORT_FUNCTIONS: systemeventlog.SystemEventInfo{EVENTID_EXPORT_FUNCTIONS, "Export Functions"},
	}

	unrecognizedSystemEventId = errors.New("Unrecognized System Event Id")
)

func GetSystemEventInfo(eventId systemeventlog.EventId) (systemeventlog.SystemEventInfo, error) {

	sei, found := eventingSystemEvents[eventId]
	if !found {

		return systemeventlog.SystemEventInfo{}, unrecognizedSystemEventId
	}

	return sei, nil
}

func LogSystemEvent(sel systemeventlog.SystemEventLogger, eventId systemeventlog.EventId,
	severity systemeventlog.EventSeverity, extraAttributes interface{}) {

	sei, err := GetSystemEventInfo(eventId)
	if err != nil {
		logging.Errorf("LogSystemEvent: Unrecognized SystemEventId: %v", eventId)
		return
	}

	se := systemeventlog.NewSystemEvent(SUB_COMPONENT_EVENTING_PRODUCER,
		sei, severity, extraAttributes)

	sel.Log(se)
}
