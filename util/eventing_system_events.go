package util

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/goutils/systemeventlog"
)

const (
	SYSTEM_EVENT_COMPONENT = "eventing"

	SUB_COMPONENT_EVENTING_PRODUCER = "eventing-producer"

	DEFAULT_TIMEOUT_SECS = 2
)

const (
	// Eventing's EventId range: 4096-5119

	EVENTID_PRODUCER_STARTUP systemeventlog.EventId = 4096

	EVENTID_CONSUMER_STARTUP systemeventlog.EventId = 4097
	EVENTID_CONSUMER_CRASH   systemeventlog.EventId = 4098

	EVENTID_START_TRACING systemeventlog.EventId = 4099
	EVENTID_STOP_TRACING  systemeventlog.EventId = 4100

	EVENTID_START_DEBUGGER systemeventlog.EventId = 4101
	EVENTID_STOP_DEBUGGER  systemeventlog.EventId = 4102

	EVENTID_CREATE_FUNCTION   systemeventlog.EventId = 4103
	EVENTID_DELETE_FUNCTION   systemeventlog.EventId = 4104
	EVENTID_IMPORT_FUNCTIONS  systemeventlog.EventId = 4105
	EVENTID_EXPORT_FUNCTIONS  systemeventlog.EventId = 4106
	EVENTID_BACKUP_FUNCTION   systemeventlog.EventId = 4107
	EVENTID_RESTORE_FUNCTION  systemeventlog.EventId = 4108
	EVENTID_DEPLOY_FUNCTION   systemeventlog.EventId = 4109
	EVENTID_UNDEPLOY_FUNCTION systemeventlog.EventId = 4110
	EVENTID_PAUSE_FUNCTION    systemeventlog.EventId = 4111
	EVENTID_RESUME_FUNCTION   systemeventlog.EventId = 4112

	EVENTID_CLEANUP_EVENTING systemeventlog.EventId = 4113
	EVENTID_DIE              systemeventlog.EventId = 4114
	EVENTID_TRIGGER_GC       systemeventlog.EventId = 4115
	EVENTID_FREE_OS_MEMORY   systemeventlog.EventId = 4116

	EVENTID_UPDATE_CONFIG    systemeventlog.EventId = 4117
	EVENTID_CLEAR_STATISTICS systemeventlog.EventId = 4118

	last_EVENTID_FOR_EVENTING systemeventlog.EventId = 5119 // Must not go beyond this.
)

var (
	eventingSystemEvents = map[systemeventlog.EventId]systemeventlog.SystemEventInfo{
		//
		EVENTID_PRODUCER_STARTUP: systemeventlog.SystemEventInfo{EVENTID_PRODUCER_STARTUP, "eventing-producer process startup"},
		//
		EVENTID_CONSUMER_STARTUP: systemeventlog.SystemEventInfo{EVENTID_CONSUMER_STARTUP, "eventing-consumer process startup"},
		EVENTID_CONSUMER_CRASH:   systemeventlog.SystemEventInfo{EVENTID_CONSUMER_CRASH, "eventing-consumer process crash"},
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
		EVENTID_IMPORT_FUNCTIONS:  systemeventlog.SystemEventInfo{EVENTID_IMPORT_FUNCTIONS, "Import Functions"},
		EVENTID_EXPORT_FUNCTIONS:  systemeventlog.SystemEventInfo{EVENTID_EXPORT_FUNCTIONS, "Export Functions"},
		EVENTID_BACKUP_FUNCTION:   systemeventlog.SystemEventInfo{EVENTID_BACKUP_FUNCTION, "Backup functions"},
		EVENTID_RESTORE_FUNCTION:  systemeventlog.SystemEventInfo{EVENTID_RESTORE_FUNCTION, "Restore functions"},
		EVENTID_DEPLOY_FUNCTION:   systemeventlog.SystemEventInfo{EVENTID_DEPLOY_FUNCTION, "Function deployed"},
		EVENTID_UNDEPLOY_FUNCTION: systemeventlog.SystemEventInfo{EVENTID_UNDEPLOY_FUNCTION, "Function undeployed"},
		EVENTID_PAUSE_FUNCTION:    systemeventlog.SystemEventInfo{EVENTID_PAUSE_FUNCTION, "Function paused"},
		EVENTID_RESUME_FUNCTION:   systemeventlog.SystemEventInfo{EVENTID_RESUME_FUNCTION, "Function resumed"},
		//
		EVENTID_CLEANUP_EVENTING: systemeventlog.SystemEventInfo{EVENTID_CLEANUP_EVENTING, "Cleanup Eventing"},
		EVENTID_DIE:              systemeventlog.SystemEventInfo{EVENTID_DIE, "Killing all eventing consumers and the eventing producer"},
		EVENTID_TRIGGER_GC:       systemeventlog.SystemEventInfo{EVENTID_TRIGGER_GC, "Trigger GC"},
		EVENTID_FREE_OS_MEMORY:   systemeventlog.SystemEventInfo{EVENTID_FREE_OS_MEMORY, "Freeing up memory to OS"},
		EVENTID_UPDATE_CONFIG:    systemeventlog.SystemEventInfo{EVENTID_UPDATE_CONFIG, "Set global eventing config"},
		EVENTID_CLEAR_STATISTICS: systemeventlog.SystemEventInfo{EVENTID_CLEAR_STATISTICS, "clear eventing function statistics"},
	}

	unrecognizedSystemEventId = errors.New("Unrecognized System Event Id")
)

var sel systemeventlog.SystemEventLogger
var sysEventOnce sync.Once

func InitialiseSystemEventLogger(restPort string) {
	sysEventOnce.Do(func() {
		baseNsserverURL := "http://" + net.JoinHostPort(Localhost(), restPort)

		sel = systemeventlog.NewSystemEventLogger(systemeventlog.SystemEventLoggerConfig{}, baseNsserverURL,
			SYSTEM_EVENT_COMPONENT, http.Client{Timeout: DEFAULT_TIMEOUT_SECS * time.Second},
			func(message string) {
				logging.Errorf(message)
			})
	})
}

func GetSystemEventInfo(eventId systemeventlog.EventId) (systemeventlog.SystemEventInfo, error) {

	sei, found := eventingSystemEvents[eventId]
	if !found {

		return systemeventlog.SystemEventInfo{}, unrecognizedSystemEventId
	}

	return sei, nil
}

func LogSystemEvent(eventId systemeventlog.EventId,
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
