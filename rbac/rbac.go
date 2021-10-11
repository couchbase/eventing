package rbac

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/util"
)

type Permission uint8

const (
	EventingManage Permission = iota
	BucketRead
	BucketWrite
	BucketDcp
)

// Known permissions
const (
	// EventingPermissionManage for auditing
	EventingManagePermission = "cluster.eventing.functions!manage"
	EventingPermissionStats  = "cluster.admin.internal.stats!read"
	ClusterPermissionRead    = "cluster.admin.security!read"
)

var (
	EventingPermissionManage = []string{EventingManagePermission}
	EventingReadPermissions  = []string{EventingManagePermission, ClusterPermissionRead}
	EventingStatsPermission  = []string{EventingPermissionStats}
)

func GetPermissions(keySpace common.Keyspace, perm Permission) (perms []string) {
	perms = make([]string, 0, 3)
	switch perm {
	case EventingManage:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].eventing.function!manage", keyspaceToRbacString(keySpace)))

	case BucketRead:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!read", keyspaceToRbacString(keySpace)))

	case BucketWrite:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!insert", keyspaceToRbacString(keySpace)))
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!upsert", keyspaceToRbacString(keySpace)))
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!delete", keyspaceToRbacString(keySpace)))

	case BucketDcp:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.dcpstream!read", keyspaceToRbacString(keySpace)))

	}
	return
}

func keyspaceToRbacString(keyspace common.Keyspace) string {
	return fmt.Sprintf("%s:%s:%s", replaceDefault(keyspace.BucketName), replaceDefault(keyspace.ScopeName), replaceDefault(keyspace.CollectionName))
}

func replaceDefault(s string) string {
	if s == "" {
		return "."
	}
	return s
}

func IsAllowed(req *http.Request, permissions []string, union bool) ([]string, error) {
	return authenticateAndCheck(req, permissions, union)
}

// Return true if all the permissions are satisfied for this user or not
// Error maybe the cbauth http server problem
// TODO: If cbauth supports IsAllowed(user, permission we don't have to
// recreate all the request and all
// If union is true then all permission to should be satisfied
func HasPermissions(owner *common.Owner, permissions []string, union bool) ([]string, error) {
	req, err := http.NewRequest(http.MethodGet, "", nil)
	if err != nil {
		return nil, err
	}

	clusterURL := net.JoinHostPort(util.Localhost(), util.GetRestPort())
	user, password, err := cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, password)

	onBehalfUser := encodeCbOnBehalfOfHeader(owner)
	req.Header.Set("cb-on-behalf-of", onBehalfUser)
	return authenticateAndCheck(req, permissions, union)
}

func AuthWebCreds(w http.ResponseWriter, req *http.Request) (cbauth.Creds, error) {
	cred, err := cbauth.AuthWebCreds(req)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		audit.Log(auditevent.AuthenticationFailure, req, nil)
		return nil, err
	}
	return cred, nil
}

func IsAllowedCreds(cred cbauth.Creds, permissions []string, union bool) ([]string, error) {
	return authorizeFromCreds(cred, permissions, union)
}

func authenticateAndCheck(req *http.Request, permissions []string, union bool) ([]string, error) {
	creds, err := cbauth.AuthWebCreds(req)
	if err != nil {
		return nil, err
	}

	return authorizeFromCreds(creds, permissions, union)
}

// Returns permission which is not allowed
// all: all the permission should be satisfied
func authorizeFromCreds(cred cbauth.Creds, permissions []string, all bool) ([]string, error) {
	notAllowed := make([]string, 0, len(permissions))
	for _, perm := range permissions {
		allowed, err := cred.IsAllowed(perm)
		if err != nil {
			return nil, err
		}
		if allowed && !all {
			return nil, nil
		}
		if !allowed {
			notAllowed = append(notAllowed, perm)
		}
	}
	if len(notAllowed) == 0 {
		return nil, nil
	}
	return notAllowed, fmt.Errorf("One or more requested permissions not present")
}

func encodeCbOnBehalfOfHeader(owner *common.Owner) (header string) {
	header = base64.StdEncoding.EncodeToString([]byte(owner.User + ":" + owner.Domain))
	return
}

// For eventing different permissions
func HandlerGetPermissions(keySpace common.Keyspace) []string {
	perms := GetPermissions(keySpace, EventingManage)
	perms = append(perms, EventingManagePermission)
	perms = append(perms, ClusterPermissionRead)

	return perms
}

func HandlerManagePermissions(keyspace common.Keyspace) []string {
	perms := GetPermissions(keyspace, EventingManage)
	return perms
}

func HandlerBucketPermissions(srcKeyspace, metaKeyspace common.Keyspace) []string {
	perms := make([]string, 0, 4)
	perms = append(perms, GetPermissions(srcKeyspace, BucketDcp)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketRead)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketWrite)...)

	return perms
}

func ValidateAuth(w http.ResponseWriter, r *http.Request, perms []string, all bool) bool {
	creds, err := cbauth.AuthWebCreds(r)
	if err != nil || creds == nil {
		w.WriteHeader(http.StatusUnauthorized)
		audit.Log(auditevent.AuthenticationFailure, r, nil)
		return false
	}

	notAllowed, err := IsAllowedCreds(creds, perms, all)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		cbauth.SendForbidden(w, notAllowed[0])
		audit.Log(auditevent.AuthorizationFailure, r, nil)
		return false
	}
	return true
}

func ValidateAuthForOp(r *http.Request, rPerms []string, mPerms []string, all bool) bool {
	cred, err := cbauth.AuthWebCreds(r)
	if err != nil {
		return false
	}

	perms := rPerms
	if r.Method != "GET" {
		perms = mPerms
	}
	_, err = IsAllowedCreds(cred, perms, all)
	return (err == nil)
}
