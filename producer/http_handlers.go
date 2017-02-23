package producer

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
)

// AggregateTaskProgress reports back collated progress from all producers
func (p *Producer) AggregateTaskProgress(w http.ResponseWriter, r *http.Request) {
	producerHostPorts := util.ListChildren(p.MetaKvAppHostPortsPath)
	logging.Infof("PRDR[%s:%d] Registered eventing nodes: %v", p.AppName, p.LenRunningConsumers(), producerHostPorts)

	var appAggProgress float64
	wg := &sync.WaitGroup{}

	netClient := &http.Client{
		Timeout: HTTPRequestTimeout,
	}

	wg.Add(len(producerHostPorts))
	for _, producerHostPort := range producerHostPorts {
		url := fmt.Sprintf("http://%s/getRebalanceStatus", producerHostPort)

		go func(url string, appAggProgress float64) {
			defer wg.Done()

			res, err := netClient.Get(url)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to gather task status from url: %s, err: %v", p.AppName, p.LenRunningConsumers(), url, err)
				return
			}
			defer res.Body.Close()

			buf, err := ioutil.ReadAll(res.Body)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to read response body from url: %s, err: %v", p.AppName, p.LenRunningConsumers(), url, err)
				return
			}

			progress, err := strconv.ParseFloat(string(buf), 64)
			if err != nil {
				logging.Errorf("PRDR[%s:%d] Failed to parse progress from url: %s, err: %v", p.AppName, p.LenRunningConsumers(), url, err)
				return
			}

			appAggProgress *= progress
		}(url, appAggProgress)
	}

	wg.Wait()
	fmt.Fprintf(w, "%v", appAggProgress)
}

// RebalanceStatus assists in reporting back progress to ns_server via cbauth_service
func (p *Producer) RebalanceStatus(w http.ResponseWriter, r *http.Request) {
	aggProgress := 1.0

	for _, consumer := range p.runningConsumers {
		consumerProgress := consumer.RebalanceTaskProgress()
		aggProgress *= consumerProgress
	}

	fmt.Fprintf(w, "%v", aggProgress)
}

// DcpEventsRemainingToProcess writes remaining dcp events to process
func (p *Producer) DcpEventsRemainingToProcess(w http.ResponseWriter, r *http.Request) {
	var remainingEvents uint64

	for _, consumer := range p.runningConsumers {
		remainingEvents += consumer.DcpEventsRemainingToProcess()
	}

	fmt.Fprintf(w, "%v", remainingEvents)
}

// GetSettings dumps the event handler specific config
func (p *Producer) GetSettings(w http.ResponseWriter, r *http.Request) {
	encodedSettings, err := json.Marshal(p.app.Settings)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode event handler settings")
		return
	}
	fmt.Fprintf(w, "%s", string(encodedSettings))
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
		return
	}
	fmt.Fprintf(w, "%s", string(encodedWorkerMap))
}

// GetNodeMap dumps vbucket distribution across eventing nodes
func (p *Producer) GetNodeMap(w http.ResponseWriter, r *http.Request) {
	encodedEventingMap, err := json.Marshal(p.vbEventingNodeAssignMap)
	if err != nil {
		fmt.Fprintf(w, "Failed to encode worker vbucket map")
		return
	}
	fmt.Fprintf(w, "%s", string(encodedEventingMap))
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
		return
	}
	fmt.Fprintf(w, "%s", string(encodedVbStats))
}
