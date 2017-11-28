// +build !enterprise

package audit

func Init(restPort string) error {
	return nil
}

func AuditLog(event interface{}, req *interface{}, context interface{}) error {
	return nil
}
