// +build enterprise

//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package audit

import (
	"fmt"
	"net"
	"net/http"

	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/util"
	goadt "github.com/couchbase/goutils/go-cbaudit"
)

type AuditEntry struct {
	goadt.GenericFields
	Context string `json:"context"`
}

var auditService *goadt.AuditSvc

func Init(restPort string) error {
	clusterURL := "http://" + net.JoinHostPort(util.Localhost(), restPort)
	svc, err := goadt.NewAuditSvc(clusterURL)
	if err != nil {
		logging.Errorf("Audit initialization failed: %v", err)
		return err
	}
	auditService = svc
	return nil
}

func Log(event auditevent.AuditEvent, req *http.Request, context interface{}) error {
	entry := AuditEntry{
		GenericFields: goadt.GetAuditBasicFields(req),
		Context:       fmt.Sprintf("%v", context),
	}
	if auditService == nil {
		logging.Debugf("Audit event without audit service: %ru", entry)
		return nil
	}
	err := auditService.Write(uint32(event), entry)
	if err != nil {
		logging.Warnf("Audit event %ru lost due to %v", entry, err)
		return err
	}
	return nil
}
