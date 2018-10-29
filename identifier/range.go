package identifier

import (
	"strings"

	"github.com/Masterminds/semver"
)

type Range struct {
	raw      string
	rawRange string
	vendor   string
	name     string
	Range    *semver.Constraints
}

// Assert Range implements Qualified ID interface.
var _ Qualified = (*Range)(nil)

func NewRange(vendor, name, rng string) (*Range, error) {
	r, err := semver.NewConstraint(rng)
	if err != nil {
		return nil, err
	}

	return &Range{
		raw:      vendor + "." + name + "@" + rng,
		rawRange: rng,
		vendor:   vendor,
		name:     name,
		Range:    r,
	}, nil
}

func (id *Range) String() string {
	return id.raw
}

func (id *Range) Matches(other ID) bool {
	return id.compare(other) == 0
}

func (id *Range) MatchesOpt(other ID, opt MatchOptions) bool {
	return id.compareOpt(other, opt) == 0
}

func (id *Range) LessThan(other ID) bool {
	return id.compare(other) < 0
}

func (id *Range) compare(other ID) int {
	return id.compareOpt(other, MatchOptions{})
}

func (id *Range) compareOpt(other ID, opt MatchOptions) int {
	switch other := other.(type) {
	case *Range:
		return strings.Compare(id.raw, other.raw)
	case *Version:
		pidComparison := strings.Compare(id.Prefix(), other.Prefix())
		if pidComparison != 0 {
			return pidComparison
		}

		version := other.Version
		if opt.IgnorePrerelease {
			version = StripPrerelease(version)
		}
		if id.Range.Check(version) {
			return 0
		}
		return strings.Compare(id.rawRange, other.rawVersion)
	case *Tag:
		return strings.Compare(id.Prefix(), other.Prefix())
	case *Partial:
		return strings.Compare(id.Prefix(), other.String())
	}
	return 1
}

func (id *Range) Prefix() string {
	return id.vendor + "." + id.name
}

func (id *Range) Suffix() string {
	return id.rawRange
}

func (id *Range) Vendor() string {
	return id.vendor
}

func (id *Range) Name() string {
	return id.name
}

func (id *Range) ToPartial() *Partial {
	return NewPartial(id.vendor, id.name)
}
