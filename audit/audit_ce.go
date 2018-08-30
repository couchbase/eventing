// +build !enterprise

package audit

// Init function
func Init(restPort string) error {
	return nil
}

// Log audit requests
func Log(event interface{}, req interface{}, context interface{}) error {
	return nil
}
