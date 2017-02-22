package producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/couchbase/eventing/util"
)

// GetSettings dumps the event handler specific config
func (p *Producer) GetSettings(w http.ResponseWriter, r *http.Request) {
	encodedSettings, err := json.Marshal(p.app.Settings)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode event handler settings")
	} else {
		fmt.Fprintf(w, "%s", string(encodedSettings))
	}
}

// UpdateSettings updates the event handler settings
func (p *Producer) UpdateSettings(w http.ResponseWriter, r *http.Request) {
	reqBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Fprintf(w, "Failed to read contents from request body")
		return
	}

	appSettings := make(map[string]interface{})
	err = json.Unmarshal(reqBody, &appSettings)
	if err != nil {
		fmt.Fprintf(w, "Failed to unmarshal contents from request body")
		return
	}

	err = util.MetakvSet(MetaKvAppSettingsPath+p.AppName, reqBody, nil)
	if err != nil {
		fmt.Fprintf(w, "Failed to store new handler setting in metakv")
		return
	}

	fmt.Fprintf(w, "Successfully written new settings, app: %s will be reloaded with new config", p.AppName)
}

// GetWorkerMap dumps the vbucket distribution across V8 workers
func (p *Producer) GetWorkerMap(w http.ResponseWriter, r *http.Request) {
	encodedWorkerMap, err := json.Marshal(p.workerVbucketMap)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode worker vbucket map")
	} else {
		fmt.Fprintf(w, "%s", string(encodedWorkerMap))
	}
}

// GetNodeMap dumps vbucket distribution across eventing nodes
func (p *Producer) GetNodeMap(w http.ResponseWriter, r *http.Request) {
	encodedEventingMap, err := json.Marshal(p.vbEventingNodeAssignMap)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode worker vbucket map")
	} else {
		fmt.Fprintf(w, "%s", string(encodedEventingMap))
	}
}

// GetConsumerVbProcessingStats dumps internal state of vbucket specific details, which is what's written to metadata bucket as well
func (p *Producer) GetConsumerVbProcessingStats(w http.ResponseWriter, r *http.Request) {
	vbStats := make(map[string]map[uint16]map[string]interface{}, 0)

	for _, consumer := range p.runningConsumers {
		consumerName := consumer.ConsumerName()
		stats := consumer.VbProcessingStats()
		vbStats[consumerName] = stats
	}

	encodedVbStats, err := json.Marshal(vbStats)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode consumer vbstats")
	} else {
		fmt.Fprintf(w, "%s", string(encodedVbStats))
	}
}
