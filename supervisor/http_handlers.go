package supervisor

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/couchbase/eventing/flatbuf/cfg"
	"github.com/couchbase/eventing/util"
	"github.com/couchbase/indexing/secondary/logging"
	flatbuffers "github.com/google/flatbuffers/go"
)

// FetchAppSetup provides the list of deployed event handlers
func (s *SuperSupervisor) FetchAppSetup(w http.ResponseWriter, r *http.Request) {
	appList := util.ListChildren(MetakvAppsPath)
	respData := make([]application, len(appList))
	for index, appName := range appList {

		path := MetakvAppsPath + appName
		data, err := util.MetakvGet(path)
		if err == nil {

			config := cfg.GetRootAsConfig(data, 0)

			app := new(application)
			app.AppHandlers = string(config.AppCode())
			app.Name = string(config.AppName())
			app.ID = int(config.Id())

			d := new(cfg.DepCfg)
			depcfg := new(depCfg)
			dcfg := config.DepCfg(d)

			depcfg.Auth = string(dcfg.Auth())
			depcfg.MetadataBucket = string(dcfg.MetadataBucket())
			depcfg.SourceBucket = string(dcfg.SourceBucket())

			var buckets []bucket
			b := new(cfg.Bucket)
			for i := 0; i < dcfg.BucketsLength(); i++ {

				if dcfg.Buckets(b, i) {
					newBucket := bucket{
						Alias:      string(b.Alias()),
						BucketName: string(b.BucketName()),
					}
					buckets = append(buckets, newBucket)
				}
			}
			depcfg.Buckets = buckets
			app.DeploymentConfig = *depcfg

			respData[index] = *app
		}
	}

	data, err := json.Marshal(respData)
	if err != nil {
		fmt.Fprintf(w, "Failed to marshal response for get_application, err: %v", err)
		return
	}
	fmt.Fprintf(w, "%s\n", data)
}

// StoreAppSetup stores updates copy of event handler definition
func (s *SuperSupervisor) StoreAppSetup(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	appName := values["name"][0]

	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errString := fmt.Sprintf("App: %s, failed to read content from http request body", appName)
		logging.Errorf("%s, err: %v", errString, err)
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	var app application
	err = json.Unmarshal(content, &app)
	if err != nil {
		errString := fmt.Sprintf("App: %s, Failed to unmarshal payload", appName)
		logging.Errorf("%s, err: %v", errString, err)
		fmt.Fprintf(w, "%s\n", errString)
		return
	}

	builder := flatbuffers.NewBuilder(0)

	var bNames []flatbuffers.UOffsetT

	for i := 0; i < len(app.DeploymentConfig.Buckets); i++ {
		alias := builder.CreateString(app.DeploymentConfig.Buckets[i].Alias)
		bName := builder.CreateString(app.DeploymentConfig.Buckets[i].BucketName)

		cfg.BucketStart(builder)
		cfg.BucketAddAlias(builder, alias)
		cfg.BucketAddBucketName(builder, bName)
		csBucket := cfg.BucketEnd(builder)

		bNames = append(bNames, csBucket)
	}

	cfg.DepCfgStartBucketsVector(builder, len(bNames))
	for i := 0; i < len(bNames); i++ {
		builder.PrependUOffsetT(bNames[i])
	}
	buckets := builder.EndVector(len(bNames))

	auth := builder.CreateString(app.DeploymentConfig.Auth)
	metaBucket := builder.CreateString(app.DeploymentConfig.MetadataBucket)
	sourceBucket := builder.CreateString(app.DeploymentConfig.SourceBucket)

	cfg.DepCfgStart(builder)
	cfg.DepCfgAddBuckets(builder, buckets)
	cfg.DepCfgAddAuth(builder, auth)
	cfg.DepCfgAddMetadataBucket(builder, metaBucket)
	cfg.DepCfgAddSourceBucket(builder, sourceBucket)
	depcfg := cfg.DepCfgEnd(builder)

	appCode := builder.CreateString(app.AppHandlers)
	aName := builder.CreateString(app.Name)

	cfg.ConfigStart(builder)
	cfg.ConfigAddId(builder, uint32(app.ID))
	cfg.ConfigAddAppCode(builder, appCode)
	cfg.ConfigAddAppName(builder, aName)
	cfg.ConfigAddDepCfg(builder, depcfg)
	config := cfg.ConfigEnd(builder)

	builder.Finish(config)

	appContent := builder.FinishedBytes()

	path := MetakvAppsPath + appName
	err = util.MetakvSet(path, appContent, nil)
	if err != nil {
		fmt.Fprintf(w, "Failed to write app config to metakv, err: %v", err)
		return
	}

	settingsPath := MetakvAppSettingsPath + appName
	sData, err := util.MetakvGet(settingsPath)
	if err != nil {
		fmt.Fprintf(w, "App: %s Failed to fetch settings from metakv, err: %v", appName, err)
		return
	}

	if sData == nil {
		settings := make(map[string]interface{})
		settings["dcp_stream_boundary"] = "everything"
		settings["tick_duration"] = DefaultStatsTickDuration
		settings["worker_count"] = DefaultWorkerCount

		mData, mErr := json.Marshal(&settings)
		if mErr != nil {
			fmt.Fprintf(w, "App: %s Failed to marshal settings, err: %v", appName, mErr)
			return
		}

		mkvErr := util.MetakvSet(settingsPath, mData, nil)
		if mkvErr != nil {
			fmt.Fprintf(w, "App: %s Failed to store updated settings in metakv, err: %v", appName, mkvErr)
			return
		}
	}

	fmt.Fprintf(w, "Stored application config in metakv")
}
