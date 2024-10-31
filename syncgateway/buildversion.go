package syncgateway

import (
	"fmt"
	"strconv"
	"strings"
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
func NewBuildVersion(version string) (BuildVersion, error) {
	major, minor, err := parseBuildVersion(version)
	if err != nil {
		return BuildVersion{}, err
	}
	return BuildVersion{
		major: major,
		minor: minor,
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

func parseBuildVersionComponents(majorStr, minorStr string) (major, minor uint8, err error) {
	if majorStr != "" {
		tmp, err := strconv.ParseUint(majorStr, 10, 8)
		if err != nil {
			return 0, 0, fmt.Errorf("couldn't parse version major: %q: %w", majorStr, err)
		}
		major = uint8(tmp)
	}

	if minorStr != "" {
		tmp, err := strconv.ParseUint(minorStr, 10, 8)
		if err != nil {
			return 0, 0, fmt.Errorf("couldn't parse version minor: %q: %w", minorStr, err)
		}
		minor = uint8(tmp)
	}
	return major, minor, nil
}

func parseBuildVersion(version string) (major, minor uint8, err error) {
	majorStr, minorStr, err := extractBuildVersionComponents(version)
	if err != nil {
		return 0, 0, err
	}
	return parseBuildVersionComponents(majorStr, minorStr)
}

func (ver BuildVersion) String() string {
	return fmt.Sprintf("%v.%v", ver.major, ver.minor)
}

func (pv BuildVersion) Equal(b BuildVersion) bool {
	return pv.major == b.major &&
		pv.minor == b.minor
}

func (pv BuildVersion) LessThan(b BuildVersion) bool {
	return (pv.major < b.major) ||
		(pv.major == b.major && pv.minor < b.minor)
}
