package supervisor2

import (
	"os"
	"path/filepath"

	"github.com/couchbase/cbauth/metakv"
	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/logging"
)

func recoverMetakvPaths(path string, cb func(path string, payload []byte) error) error {
	children, err := metakv.ListAllChildren(path)
	if err != nil {
		return err
	}

	for _, child := range children {
		err := cb(child.Path, child.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

func renameLogFile(function *application.FunctionDetails, eventingDir string) {
	logFileDir, newFileName := application.GetLogDirectoryAndFileName(false, function, eventingDir)
	oldLogFileDir, oldFileName := application.GetLogDirectoryAndFileName(true, function, eventingDir)
	topLocDir := application.GetEventingDir(function, eventingDir)

	topLocDir, err := filepath.Abs(topLocDir)
	if err != nil {
		logging.Errorf("Error getting absolute path for toplevel directory %s err: %v", logFileDir, err)
		err = os.MkdirAll(logFileDir, 0755)
		if err != nil {
			logging.Errorf("Error creating log file directory for %s err: %v", function.AppLocation, err)
		}
		return
	}
	err = os.MkdirAll(topLocDir, 0755)
	if err != nil {
		logging.Errorf("Error creating top level directory for %s err: %v", function.AppLocation, err)
	}

	logFileDir, err = filepath.Abs(logFileDir)
	if err != nil {
		logging.Errorf("Error getting absolute path for log file dir %s err: %v", logFileDir, err)
		err = os.MkdirAll(logFileDir, 0755)
		if err != nil {
			logging.Errorf("Error creating log file directory for %s err: %v", function.AppLocation, err)
		}
		return
	}

	oldLogFileDir, err = filepath.Abs(oldLogFileDir)
	if err != nil {
		logging.Errorf("Error getting absolute path for old file dir %s err: %v", oldLogFileDir, err)
		err = os.MkdirAll(logFileDir, 0755)
		if err != nil {
			logging.Errorf("Error creating log file directory for %s err: %v", function.AppLocation, err)
		}
		return
	}

	if logFileDir == topLocDir {
		if err = os.Rename(oldFileName, newFileName); err != nil && !os.IsNotExist(err) {
			logging.Warnf("Error renaming old log file %s to %s err: %v", oldFileName, newFileName, err)
		}
		return
	}

	// rename b_[bucketname]/s_[scopename]
	oldParent := filepath.Dir(oldLogFileDir)
	parent := filepath.Dir(logFileDir)
	if err = os.Rename(oldParent, parent); err != nil && !os.IsNotExist(err) {
		logging.Warnf("Error renaming old log file directory %s to %s err: %v", oldParent, parent, err)
	}

	base := filepath.Base(oldLogFileDir)
	oldLogFileDir = filepath.Join(parent, base)
	if err = os.Rename(oldLogFileDir, logFileDir); err != nil && !os.IsNotExist(err) {
		logging.Warnf("Error renaming old log file directory %s to %s err: %v", oldLogFileDir, logFileDir, err)
	}

	if err = os.MkdirAll(logFileDir, 0755); err != nil {
		logging.Errorf("Error creating log file directory for %s err: %v", function.AppLocation, err)
	}

	base = filepath.Base(oldFileName)
	oldFileName = filepath.Join(logFileDir, base)
	if err = os.Rename(oldFileName, newFileName); err != nil && !os.IsNotExist(err) {
		logging.Warnf("Error renaming old log file %s to %s err: %v", oldFileName, newFileName, err)
	}
}
