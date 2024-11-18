package rbac

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/eventing/application"
)

var (
	ErrAuthorisation  = errors.New("one or more requested permissions missing")
	ErrUserDeleted    = errors.New("user deleted")
	ErrAuthentication = errors.New("unauthenticated User")
)

var (
	clusterURL = ""
)

// InitRbacManager initilaise the rbac module
// Should call this method before any other method is invoked
func InitRbacManager(url string) {
	clusterURL = url
}

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

func keyspaceToRbacString(keyspace application.Keyspace) string {
	return fmt.Sprintf("%s:%s:%s", defaultIfUnset(keyspace.BucketName), defaultIfUnset(keyspace.ScopeName), defaultIfUnset(keyspace.CollectionName))
}

func defaultIfUnset(s string) string {
	if s == "" || s == "*" {
		return "*"
	}
	return s
}

// Check for user credentials
func authCreds(req *http.Request) (cbauth.Creds, error) {
	cred, err := cbauth.AuthWebCreds(req)
	if err == cbauth.ErrNoAuth {
		return nil, ErrAuthentication
	}

	if err != nil {
		// for some reason cbauth does not return ErrNoAuth when no credentials are provided. Only does when
		// the provided credentials are invalid. Manually check for the no credentials and return err.
		if err.Error() == "no web credentials found in request" {
			return nil, ErrAuthentication
		}
		return nil, err
	}

	return cred, nil
}

// Returns some or all permissions that are required for performing an action may be missing
// all: all the permission should be satisfied
func authorizeFromCreds(cred cbauth.Creds, permissions []string, all bool) ([]string, error) {
	missingPerms := make([]string, 0, len(permissions))
	for _, perm := range permissions {
		allowed, err := cred.IsAllowed(perm)
		if err != nil {
			return nil, err
		}
		if allowed && !all {
			return nil, nil
		}
		if !allowed {
			missingPerms = append(missingPerms, perm)
		}
	}

	if len(missingPerms) == 0 {
		return nil, nil
	}
	return missingPerms, ErrAuthorisation
}

// Authenticate and check for permission
func isAllowed(req *http.Request, permissions []string, all bool) ([]string, error) {
	cred, err := authCreds(req)
	if err != nil {
		return nil, err
	}

	return authorizeFromCreds(cred, permissions, all)
}

func encodeCbOnBehalfOfHeader(owner *application.Owner) (header string) {
	return encodeCbOnBehalfOfHeaderWithUser(owner.User, owner.Domain)
}

func encodeCbOnBehalfOfHeaderWithUser(user, domain string) (header string) {
	header = base64.StdEncoding.EncodeToString([]byte(user + ":" + domain))
	return
}

// Exported functions
// For eventing different permissions
func HandlerGetPermissions(keyspace application.Keyspace) []string {
	perms := GetPermissions(keyspace, EventingManage)
	perms = append(perms, EventingManagePermission)
	perms = append(perms, ClusterPermissionRead)
	return perms
}

func HandlerManagePermissions(keyspace application.Keyspace) []string {
	perms := GetPermissions(keyspace, EventingManage)
	return perms
}

func HandlerBucketPermissions(srcKeyspace, metaKeyspace application.Keyspace) []string {
	perms := make([]string, 0, 5)
	perms = append(perms, GetPermissions(srcKeyspace, BucketDcp)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketRead)...)
	perms = append(perms, GetPermissions(metaKeyspace, BucketWrite)...)
	return perms
}

func GetPermissions(keyspace application.Keyspace, perm Permission) (perms []string) {
	perms = make([]string, 0, 3)
	switch perm {
	case EventingManage:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].eventing.function!manage", keyspaceToRbacString(keyspace)))

	case BucketRead:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!read", keyspaceToRbacString(keyspace)))

	case BucketWrite:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!insert", keyspaceToRbacString(keyspace)))
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!upsert", keyspaceToRbacString(keyspace)))
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.docs!delete", keyspaceToRbacString(keyspace)))

	case BucketDcp:
		perms = append(perms, fmt.Sprintf("cluster.collection[%s].data.dcpstream!read", keyspaceToRbacString(keyspace)))

	}
	return
}

func IsAllowed(req *http.Request, permissions []string, all bool) ([]string, error) {
	return isAllowed(req, permissions, all)
}

func IsAllowedCreds(cred cbauth.Creds, permissions []string, all bool) ([]string, error) {
	return authorizeFromCreds(cred, permissions, all)
}

// Return true if all the permissions are satisfied for this user or not
// Error maybe the cbauth http server problem
// TODO: If cbauth supports IsAllowed(user, permission) we don't have to
// recreate all the request and all
// If all is true then all permission to should be satisfied
func HasPermissions(owner *application.Owner, permissions []string, all bool) ([]string, error) {
	// No user running as admin
	if owner.User == "" && owner.Domain == "" {
		return nil, nil
	}

	if owner.UUID != "" {
		uuid, err := cbauth.GetUserUuid(owner.User, owner.Domain)
		if err == cbauth.ErrNoUuid {
			return permissions, ErrUserDeleted
		}
		if err != nil {
			return nil, err
		}

		if uuid != owner.UUID {
			return permissions, ErrUserDeleted
		}
	}

	req, err := http.NewRequest(http.MethodGet, "", nil)
	if err != nil {
		return nil, err
	}

	user, password, err := cbauth.GetHTTPServiceAuth(clusterURL)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, password)

	onBehalfUser := encodeCbOnBehalfOfHeader(owner)
	req.Header.Set("cb-on-behalf-of", onBehalfUser)
	return isAllowed(req, permissions, all)
}

func AuthWebCreds(req *http.Request) (cbauth.Creds, error) {
	return authCreds(req)
}

func ValidateAuthForOp(r *http.Request, rPerms []string, mPerms []string, all bool) ([]string, error) {
	perms := rPerms
	if r.Method != "GET" {
		perms = mPerms
	}

	missingPerms, err := isAllowed(r, perms, all)
	if err != nil {
		return missingPerms, err
	}

	return nil, nil
}

func PutOnBehalfOf(cred cbauth.Creds, query map[string][]string) map[string][]string {
	if query == nil {
		query = make(map[string][]string)
	}
	query["cb-on-behalf-of"] = []string{encodeCbOnBehalfOfHeaderWithUser(cred.Name(), cred.Domain())}
	return query
}
