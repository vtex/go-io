package identifier

import (
	"strconv"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
)

type Major int64

type Version struct {
	raw        string
	rawVersion string
	vendor     string
	name       string
	Version    *semver.Version
}

// Assert Version implements Qualified ID interface.
var _ Qualified = (*Version)(nil)

func NewVersion(vendor, name, version string) (*Version, error) {
	id := &Version{}
	if err := id.init(vendor, name, version); err != nil {
		return nil, err
	}
	return id, nil
}

func (id *Version) init(vendor, name, version string) error {
	v, err := semver.NewVersion(version)
	if err != nil {
		return err
	}

	*id = Version{
		raw:        vendor + "." + name + "@" + version,
		rawVersion: version,
		vendor:     vendor,
		name:       name,
		Version:    v,
	}
	return nil
}

func (id *Version) String() string {
	return id.raw
}

func (id *Version) Major() Major {
	return Major(id.Version.Major())
}

func (id *Version) Matches(other ID) bool {
	return id.compare(other) == 0
}

func (id *Version) MatchesOpt(other ID, opt MatchOptions) bool {
	return id.compareOpt(other, opt) == 0
}

func (id *Version) LessThan(other ID) bool {
	return id.compare(other) < 0
}

func (id *Version) compare(other ID) int {
	return id.compareOpt(other, MatchOptions{})
}

func (id *Version) compareOpt(other ID, opt MatchOptions) int {
	version := id.Version
	if opt.IgnorePrerelease {
		version = StripPrerelease(version)
	}
	switch other := other.(type) {
	case *Version:
		pidComparison := strings.Compare(id.Prefix(), other.Prefix())
		if pidComparison != 0 {
			return pidComparison
		}
		otherVersion := other.Version
		if opt.IgnorePrerelease {
			otherVersion = StripPrerelease(otherVersion)
		}
		return version.Compare(otherVersion)
	case *Range:
		pidComparison := strings.Compare(id.Prefix(), other.Prefix())
		if pidComparison == 0 {
			if other.Range.Check(version) {
				return 0
			}
			return strings.Compare(version.String(), other.rawRange)
		}
		return pidComparison
	case *Tag:
		return strings.Compare(id.Prefix(), other.Prefix())
	case *Partial:
		return strings.Compare(id.Prefix(), other.String())
	}
	return 1
}

func (id *Version) Prefix() string {
	return id.vendor + "." + id.name
}

func (id *Version) Suffix() string {
	return id.rawVersion
}

func (id *Version) Vendor() string {
	return id.vendor
}

func (id *Version) Name() string {
	return id.name
}

func (id *Version) ToPartial() *Partial {
	return NewPartial(id.vendor, id.name)
}

func (id *Version) ToMajorID() *Range {
	rng := strconv.FormatInt(int64(id.Version.Major()), 10) + ".x"
	rangeID, _ := NewRange(id.vendor, id.name, rng)
	return rangeID
}

func (id *Version) HasSameMajor(other *Version) bool {
	return id.Version.Major() == other.Version.Major()
}

func (id *Version) UnmarshalText(b []byte) error {
	matches := semverIDRegex.FindStringSubmatch(string(b))
	if len(matches) == 0 {
		return errors.Errorf("Invalid ID format, must match expression: %s", semverIDRegex.String())
	}
	return id.init(matches[1], matches[2], matches[3])
}

func (id *Version) MarshalText() ([]byte, error) {
	if id.raw == "" {
		return nil, errors.New("Cannot marshal uninitialized ID")
	}
	return []byte(id.raw), nil
}
