package common

import (
	"errors"
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
		EVENTID_PRODUCER_STARTUP: systemeventlog.SystemEventInfo{EventId: EVENTID_PRODUCER_STARTUP, Description: "eventing-producer process startup"},
		//
		EVENTID_CONSUMER_STARTUP: systemeventlog.SystemEventInfo{EventId: EVENTID_CONSUMER_STARTUP, Description: "eventing-consumer process startup"},
		EVENTID_CONSUMER_CRASH:   systemeventlog.SystemEventInfo{EventId: EVENTID_CONSUMER_CRASH, Description: "eventing-consumer process crash"},
		//
		EVENTID_START_TRACING: systemeventlog.SystemEventInfo{EventId: EVENTID_START_TRACING, Description: "Tracing started"},
		EVENTID_STOP_TRACING:  systemeventlog.SystemEventInfo{EventId: EVENTID_STOP_TRACING, Description: "Tracing stopped"},
		//
		EVENTID_START_DEBUGGER: systemeventlog.SystemEventInfo{EventId: EVENTID_START_DEBUGGER, Description: "Debugger started"},
		EVENTID_STOP_DEBUGGER:  systemeventlog.SystemEventInfo{EventId: EVENTID_STOP_DEBUGGER, Description: "Debugger stopped"},
		//
		EVENTID_CREATE_FUNCTION: systemeventlog.SystemEventInfo{EventId: EVENTID_CREATE_FUNCTION, Description: "Create Function"},
		EVENTID_DELETE_FUNCTION: systemeventlog.SystemEventInfo{EventId: EVENTID_DELETE_FUNCTION, Description: "Delete Function"},
		//
		EVENTID_IMPORT_FUNCTIONS:  systemeventlog.SystemEventInfo{EventId: EVENTID_IMPORT_FUNCTIONS, Description: "Import Functions"},
		EVENTID_EXPORT_FUNCTIONS:  systemeventlog.SystemEventInfo{EventId: EVENTID_EXPORT_FUNCTIONS, Description: "Export Functions"},
		EVENTID_BACKUP_FUNCTION:   systemeventlog.SystemEventInfo{EventId: EVENTID_BACKUP_FUNCTION, Description: "Backup functions"},
		EVENTID_RESTORE_FUNCTION:  systemeventlog.SystemEventInfo{EventId: EVENTID_RESTORE_FUNCTION, Description: "Restore functions"},
		EVENTID_DEPLOY_FUNCTION:   systemeventlog.SystemEventInfo{EventId: EVENTID_DEPLOY_FUNCTION, Description: "Function deployed"},
		EVENTID_UNDEPLOY_FUNCTION: systemeventlog.SystemEventInfo{EventId: EVENTID_UNDEPLOY_FUNCTION, Description: "Function undeployed"},
		EVENTID_PAUSE_FUNCTION:    systemeventlog.SystemEventInfo{EventId: EVENTID_PAUSE_FUNCTION, Description: "Function paused"},
		EVENTID_RESUME_FUNCTION:   systemeventlog.SystemEventInfo{EventId: EVENTID_RESUME_FUNCTION, Description: "Function resumed"},
		//
		EVENTID_CLEANUP_EVENTING: systemeventlog.SystemEventInfo{EventId: EVENTID_CLEANUP_EVENTING, Description: "Cleanup Eventing"},
		EVENTID_DIE:              systemeventlog.SystemEventInfo{EventId: EVENTID_DIE, Description: "Killing all eventing consumers and the eventing producer"},
		EVENTID_TRIGGER_GC:       systemeventlog.SystemEventInfo{EventId: EVENTID_TRIGGER_GC, Description: "Trigger GC"},
		EVENTID_FREE_OS_MEMORY:   systemeventlog.SystemEventInfo{EventId: EVENTID_FREE_OS_MEMORY, Description: "Freeing up memory to OS"},
		EVENTID_UPDATE_CONFIG:    systemeventlog.SystemEventInfo{EventId: EVENTID_UPDATE_CONFIG, Description: "Set global eventing config"},
		EVENTID_CLEAR_STATISTICS: systemeventlog.SystemEventInfo{EventId: EVENTID_CLEAR_STATISTICS, Description: "clear eventing function statistics"},
	}

	unrecognizedSystemEventId = errors.New("unrecognized System Event Id")
)

var sel systemeventlog.SystemEventLogger
var sysEventOnce sync.Once

func InitialiseSystemEventLogger(baseNsserverURL string) {
	sysEventOnce.Do(func() {
		sel = systemeventlog.NewSystemEventLogger(systemeventlog.SystemEventLoggerConfig{}, baseNsserverURL,
			SYSTEM_EVENT_COMPONENT, http.Client{Timeout: DEFAULT_TIMEOUT_SECS * time.Second},
			func(message string) {
				logging.Errorf("systemEvent::log error in while logging system event: %v", message)
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
	logPrefix := "systemEvent::LogSystemEvent"

	sei, err := GetSystemEventInfo(eventId)
	if err != nil {
		logging.Errorf("%s unrecognized SystemEventId: %v", logPrefix, eventId)
		return
	}

	se := systemeventlog.NewSystemEvent(SUB_COMPONENT_EVENTING_PRODUCER,
		sei, severity, extraAttributes)

	sel.Log(se)
}
