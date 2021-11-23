package rbac

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/audit"
	"github.com/couchbase/eventing/common"
	"github.com/couchbase/eventing/gen/auditevent"
	"github.com/couchbase/eventing/util"
)

var (
	ErrAuthorisation = errors.New("One or more requested permissions missing")
	ErrUserDeleted   = errors.New("User deleted")
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
	EventingAnyPermission    = "cluster.collection[.:.:].eventing.function!manage"
)

var (
	EventingPermissionManage         = []string{EventingManagePermission}
	EventingReadPermissions          = []string{EventingManagePermission, ClusterPermissionRead}
	EventingStatsPermission          = []string{EventingPermissionStats}
	EventingAnyManageReadPermissions = []string{EventingManagePermission, ClusterPermissionRead, EventingAnyPermission}
)

func GetPermissions(keySpace *common.Keyspace, perm Permission) (perms []string) {
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

func keyspaceToRbacString(keyspace *common.Keyspace) string {
	return fmt.Sprintf("%s:%s:%s", replaceDefault(keyspace.BucketName), replaceDefault(keyspace.ScopeName), replaceDefault(keyspace.CollectionName))
}

func replaceDefault(s string) string {
	if s == "" || s == "*" {
		return "*"
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
        if owner.UUID != "" {
                uuid, err := cbauth.GetUserUuid(owner.User, owner.Domain)
                if err != nil {
                        return nil, err
                }
                if uuid != owner.UUID {
                        return nil, ErrUserDeleted
                }
        }

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
	return authCreds(w, req, true)
}

// Maybe we can merge both
func AuthWebCredsWithoutAudit(w http.ResponseWriter, req *http.Request) (cbauth.Creds, error) {
	return authCreds(w, req, false)
}

func authCreds(w http.ResponseWriter, req *http.Request, auditAndSend bool) (cbauth.Creds, error) {
	cred, err := cbauth.AuthWebCreds(req)
	if err == cbauth.ErrNoAuth {
		if auditAndSend {
			audit.Log(auditevent.AuthenticationFailure, req, nil)
			sendUnauthenticated(w)
		}
		return nil, err
	}

	if err != nil {
		sendInternalError(w)
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
	return notAllowed, ErrAuthorisation
}

func encodeCbOnBehalfOfHeader(owner *common.Owner) (header string) {
	header = base64.StdEncoding.EncodeToString([]byte(owner.User + ":" + owner.Domain))
	return
}

// For eventing different permissions
func HandlerGetPermissions(keySpace *common.Keyspace) []string {
	perms := GetPermissions(keySpace, EventingManage)
	perms = append(perms, EventingManagePermission)
	perms = append(perms, ClusterPermissionRead)
	return perms
}

func HandlerManagePermissions(keyspace *common.Keyspace) []string {
	perms := GetPermissions(keyspace, EventingManage)
	return perms
}

func HandlerBucketPermissions(srcKeyspace, metaKeyspace *common.Keyspace) []string {
	perms := make([]string, 0, 5)
	perms = append(perms, GetPermissions(srcKeyspace, BucketDcp)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketRead)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketWrite)...)
	return perms
}

// ::TODO::Move to ForbiddenJSONMultiple and SendForbiddenMultiple to cbauth:convenience.go
// ForbiddenJSON returns json 403 response for given permissions
func forbiddenJSONMultiple(permissions []string) ([]byte, error) {
	jsonStruct := map[string]interface{}{
		"message":     "Forbidden. User needs one of the following permissions",
		"permissions": permissions,
	}
	return json.Marshal(jsonStruct)
}

// SendForbidden sends 403 Forbidden with json payload that contains list
// of required permissions to response on given response writer.
func sendForbiddenMultiple(w http.ResponseWriter, permissions []string) error {
	b, err := forbiddenJSONMultiple(permissions)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	w.Write(b)
	return nil
}

func unauthenticatedJSON() ([]byte, error) {
	jsonStruct := map[string]interface{}{
		"message": "Unauthenticated User",
	}
	return json.Marshal(jsonStruct)
}

func sendUnauthenticated(w http.ResponseWriter) error {
	b, err := unauthenticatedJSON()
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write(b)
	return nil
}

func sendInternalError(w http.ResponseWriter) {
	http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	return
}

// Send the response back to caller
// Also audit the request
func ValidateAuth(w http.ResponseWriter, r *http.Request, perms []string, all bool) bool {
	creds, err := cbauth.AuthWebCreds(r)
	if err != nil || creds == nil {
		audit.Log(auditevent.AuthenticationFailure, r, nil)
		sendUnauthenticated(w)
		return false
	}

	notAllowed, err := IsAllowedCreds(creds, perms, all)
	if err != nil {
		audit.Log(auditevent.AuthorizationFailure, r, nil)
		sendForbiddenMultiple(w, notAllowed)
		return false
	}
	return true
}

func ValidateAuthForOp(w http.ResponseWriter, r *http.Request, rPerms []string, mPerms []string, all bool) bool {
	cred, err := cbauth.AuthWebCreds(r)
	if err != nil {
		audit.Log(auditevent.AuthenticationFailure, r, nil)
		sendUnauthenticated(w)
		return false
	}

	perms := rPerms
	if r.Method != "GET" {
		perms = mPerms
	}

	notAllowed, err := IsAllowedCreds(cred, perms, all)
	if err != nil {
		audit.Log(auditevent.AuthorizationFailure, r, nil)
		sendForbiddenMultiple(w, notAllowed)
		return false
	}

	return true
}
