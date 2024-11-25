package syncgateway

import (
	"fmt"
	"strconv"
	"strings"

	ver "github.com/couchbase/eventing/notifier"
)

// stringCutBefore returns the value up to the first instance of sep if it exists, and the remaining part of the string after sep.
func stringCutBefore(s, sep string) (value, remainder string) {
	val, after, ok := strings.Cut(s, sep)
	if !ok {
		return "", s
	}
	return val, after
}

// stringCutAfter returns the value after the first instance of sep if it exists, and the remaining part of the string before sep.
func stringCutAfter(s, sep string) (value, remainder string) {
	before, val, ok := strings.Cut(s, sep)
	if !ok {
		return "", s
	}
	return val, before
}

const (
	buildVersionSep        = '.'
	buildVersionSepEpoch   = ':'
	buildVersionSepBuild   = '@'
	buildVersionSepEdition = '-'
)

// BuildVersion is an [epoch:]major.minor.patch[.other][@build][-edition] version that has methods to reliably extract information.
type BuildVersion struct {
	major, minor uint8
}

// Expected format: `[epoch:]major.minor.patch[.other][@build][-edition]`
func NewBuildVersion(version string) (defaultVer ver.Version, err error) {
	if len(version) == 0 {
		return ver.Version{
			Major: 3,
			Minor: 1,
		}, nil
	}

	major, minor, err := parseBuildVersion(version)
	if err != nil {
		return
	}
	return ver.Version{
		Major: major,
		Minor: minor,
	}, nil
}

func extractBuildVersionComponents(version string) (major, minor string, err error) {
	var remainder string

	// parse epoch and skip capture
	_, remainder = stringCutBefore(version, string(buildVersionSepEpoch))
	// parse edition and skip capture
	_, remainder = stringCutAfter(remainder, string(buildVersionSepEdition))
	// parse build number and skip capture
	_, remainder = stringCutAfter(remainder, string(buildVersionSepBuild))
	// major.minor.patch[.other]
	major, remainder = stringCutBefore(remainder, string(buildVersionSep))
	minor, _ = stringCutBefore(remainder, string(buildVersionSep))

	if major == "" || minor == "" {
		return "", "", fmt.Errorf("version %q requires at least major.minor components", version)
	}

	return major, minor, nil
}

func parseBuildVersionComponents(majorStr, minorStr string) (major, minor int, err error) {
	if majorStr != "" {
		major, err = strconv.Atoi(majorStr)
		if err != nil {
			return 0, 0, fmt.Errorf("couldn't parse version major: %q: %w", majorStr, err)
		}
	}

	if minorStr != "" {
		minor, err = strconv.Atoi(minorStr)
		if err != nil {
			return 0, 0, fmt.Errorf("couldn't parse version minor: %q: %w", minorStr, err)
		}
	}
	return major, minor, nil
}

func parseBuildVersion(version string) (major, minor int, err error) {
	majorStr, minorStr, err := extractBuildVersionComponents(version)
	if err != nil {
		return 0, 0, err
	}
	return parseBuildVersionComponents(majorStr, minorStr)
}
