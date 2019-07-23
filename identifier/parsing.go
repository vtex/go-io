package identifier

import (
	"fmt"
	"regexp"

	"github.com/pkg/errors"
)

const vendorPattern string = `[a-z][a-z0-9-]{0,48}[a-z0-9]`
const namePattern string = `[a-z][a-z0-9-]{0,126}[a-z0-9]`
const enginePattern string = vendorPattern
const tagPattern string = vendorPattern

var (
	vendorRegex    *regexp.Regexp
	nameRegex      *regexp.Regexp
	partialIDRegex *regexp.Regexp
	alienIDRegex   *regexp.Regexp
	tagIDRegex     *regexp.Regexp
	semverIDRegex  *regexp.Regexp
)

func init() {
	vendorRegex = regexp.MustCompile(fmt.Sprintf(`^%s$`, vendorPattern))
	nameRegex = regexp.MustCompile(fmt.Sprintf(`^%s$`, namePattern))
	partialIDRegex = regexp.MustCompile(fmt.Sprintf(`^(%s)\.(%s)$`, vendorPattern, namePattern))
	alienIDRegex = regexp.MustCompile(fmt.Sprintf(`^(%s):(.+?)@(.+)$`, enginePattern))
	tagIDRegex = regexp.MustCompile(fmt.Sprintf(`^(%s)\.(%s)@(%s)$`, vendorPattern, namePattern, tagPattern))
	semverIDRegex = regexp.MustCompile(fmt.Sprintf(`^(%s)\.(%s)@(.+)$`, vendorPattern, namePattern))
}

func ParseIdentifier(s string) (ID, error) {
	if matches := partialIDRegex.FindStringSubmatch(s); len(matches) != 0 {
		return NewPartial(matches[1], matches[2]), nil
	}
	if matches := alienIDRegex.FindStringSubmatch(s); len(matches) != 0 {
		return NewAlien(matches[1], matches[2], matches[3]), nil
	}
	if matches := tagIDRegex.FindStringSubmatch(s); len(matches) != 0 {
		return NewTag(matches[1], matches[2], matches[3]), nil
	}
	if matches := semverIDRegex.FindStringSubmatch(s); len(matches) != 0 {
		versionID, versionIDErr := NewVersion(matches[1], matches[2], matches[3])
		if versionID != nil {
			return versionID, nil
		}
		rangeID, rangeIDErr := NewRange(matches[1], matches[2], matches[3])
		if rangeID != nil {
			return rangeID, nil
		}
		return nil, errors.Errorf("Suffix is neither semver version (error: %v) nor constraint (error: %v)", versionIDErr, rangeIDErr)
	}
	return nil, errors.New("Couldn't parse app identifier")
}

func ParseVersion(id string) (*Version, error) {
	if id == "" {
		return nil, nil
	} else if parsedID, err := ParseIdentifier(id); err != nil {
		return nil, err
	} else if versionID, isVersion := parsedID.(*Version); !isVersion {
		return nil, errors.New("Parsed identifier is not version")
	} else {
		return versionID, nil
	}
}

func ParseRange(id string) (*Range, error) {
	if parsedID, err := ParseIdentifier(id); err != nil {
		return nil, err
	} else if rangeID, isRange := parsedID.(*Range); !isRange {
		return nil, errors.New("Parsed identifier is not range")
	} else {
		return rangeID, nil
	}
}

func ParsePartial(id string) (*Partial, error) {
	if parsedID, err := ParseIdentifier(id); err != nil {
		return nil, err
	} else if partialID, isPartial := parsedID.(*Partial); !isPartial {
		return nil, errors.New("Parsed identifier is not range")
	} else {
		return partialID, nil
	}
}

func ParseVersionParts(vendor, name, version string) (*Version, error) {
	if !vendorRegex.MatchString(vendor) {
		return nil, errors.Errorf("Invalid app ID vendor: %s", vendor)
	} else if !nameRegex.MatchString(name) {
		return nil, errors.Errorf("Invalid app ID name: %s", name)
	}

	id, err := NewVersion(vendor, name, version)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid app ID version: %s", version)
	}

	return id, nil
}

func ParseComposed(id string) (Composed, error) {
	if id == "" {
		return nil, nil
	} else if parsedID, err := ParseIdentifier(id); err != nil {
		return nil, err
	} else if composedID, isComposed := parsedID.(Composed); !isComposed {
		return nil, errors.New("Parsed identifier is not composed")
	} else {
		return composedID, nil
	}
}

func ParseQualified(id string) (Qualified, error) {
	if id == "" {
		return nil, nil
	} else if parsedID, err := ParseIdentifier(id); err != nil {
		return nil, err
	} else if qualifiedID, isQualified := parsedID.(Qualified); !isQualified {
		return nil, errors.New("Parsed identifier is not qualified")
	} else {
		return qualifiedID, nil
	}
}

func MustParse(s string) ID {
	id, err := ParseIdentifier(s)
	if err != nil {
		panic(err)
	}

	return id
}
