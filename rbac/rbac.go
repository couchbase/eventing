package rbac

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/util"
)

type Privilege uint8

const (
	EventingManage Privilege = iota
	BucketRead
	BucketWrite
)

// Known permissions
const (
	// EventingPermissionManage for auditing
	EventingPermissionManage = "cluster.eventing.functions!manage"
	EventingPermissionStats  = "cluster.admin.internal.stats!read"
	ClusterPermissionRead    = "cluster.admin.security!read"
)

func GetPermission(keySpace common.Keyspace, privilege Privilege) (perm string) {
	switch privilege {
	case EventingManage:
		perm = fmt.Sprintf("cluster.collection[%s].eventing.function!manage", keyspaceToRbacString(keySpace))
	case BucketRead:
		perm = fmt.Sprintf("cluster.collection[%s].collections!read", keyspaceToRbacString(keySpace))
	case BucketWrite:
		perm = fmt.Sprintf("cluster.collection[%s].collections!write", keyspaceToRbacString(keySpace))
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
// If union is true then all privilege to should be satisfied
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

func AuthWebCreds(req *http.Request) (cbauth.Creds, error) {
	return cbauth.AuthWebCreds(req)
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
	notAllowed := make([]string, 0, 3)
	for _, perm := range permissions {
		allowed, err := cred.IsAllowed(perm)
		if err != nil {
			notAllowed = append(notAllowed, perm)
			return notAllowed, err
		}
		if allowed && !all {
			return notAllowed, nil
		}
		if !allowed {
			notAllowed = append(notAllowed, perm)
		}
	}
	if len(notAllowed) == 0 {
		return notAllowed, nil
	}
	return notAllowed, fmt.Errorf("Few privilege is not present")
}

func encodeCbOnBehalfOfHeader(owner common.Owner) (header string) {
	header = base64.StdEncoding.EncodeToString([]byte(owner.User + ":" + owner.Domain))
	return
}

// For eventing different permissions
func HandlerGetPermissions(keySpace common.Keyspace) []string {
	managePrevilage := GetPermission(keySpace, EventingManage)
	privilege := []string{managePrevilage, EventingPermissionManage, ClusterPermissionRead}
	return privilege
}

func HandleManagePermissions(keyspace common.Keyspace) []string {
	managePrevilage := GetPermission(keyspace, EventingManage)
	privilege := []string{managePrevilage}
	return privilege
}

func HandleBucketPermissions(srcKeyspace, metaKeyspace common.Keyspace) []string {
	read := GetPermission(srcKeyspace, BucketRead)
	write := GetPermission(metaKeyspace, BucketWrite)
	return []string{read, write}
}
