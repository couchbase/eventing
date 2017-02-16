package main

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

func fetchAppSetup(w http.ResponseWriter, r *http.Request) {
	appList := util.GetAppList(MetaKvAppsPath)
	respData := make([]application, len(appList))
	for index, appName := range appList {

		path := MetaKvAppsPath + appName
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
			depcfg.TickDuration = int(dcfg.TickDuration())
			depcfg.WorkerCount = int(dcfg.WorkerCount())

			buckets := make([]bucket, 0)
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

	data, _ := json.Marshal(respData)
	fmt.Fprintf(w, "%s\n", data)
}

func storeAppSetup(w http.ResponseWriter, r *http.Request) {
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

	bNames := make([]flatbuffers.UOffsetT, 0)

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
	cfg.DepCfgAddTickDuration(builder, uint32(app.DeploymentConfig.TickDuration))
	cfg.DepCfgAddWorkerCount(builder, uint32(app.DeploymentConfig.WorkerCount))
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

	path := MetaKvAppsPath + appName
	err = util.MetakvSet(path, appContent, nil)
	if err != nil {
		fmt.Fprintf(w, "Failed to write app config to metakv, err: %v", err)
		return
	}

	fmt.Fprintf(w, "Stored application config in metakv")
}
