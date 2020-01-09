package identifier

import (
	"strings"

	"github.com/pkg/errors"
)

type Partial struct {
	raw    string
	Vendor string
	Name   string
}

// Assert Partial implements ID interface.
var _ ID = (*Partial)(nil)

func NewPartial(vendor, name string) *Partial {
	return &Partial{
		raw:    vendor + "." + name,
		Vendor: vendor,
		Name:   name,
	}
}

func (id *Partial) String() string {
	return id.raw
}

func (id *Partial) Matches(other ID) bool {
	return id.compare(other) == 0
}

func (id *Partial) LessThan(other ID) bool {
	return id.compare(other) < 0
}

func (id *Partial) compare(other ID) int {
	switch other := other.(type) {
	case Qualified:
		return strings.Compare(id.raw, other.Prefix())
	default:
		return strings.Compare(id.String(), other.String())
	}
}

func (id *Partial) AtVersion(version string) (*Version, error) {
	return NewVersion(id.Vendor, id.Name, version)
}

func (id *Partial) WithRange(constraint string) (*Range, error) {
	return NewRange(id.Vendor, id.Name, constraint)
}

func (id *Partial) UnmarshalText(b []byte) error {
	matches := partialIDRegex.FindStringSubmatch(string(b))
	if len(matches) == 0 {
		return errors.Errorf("Invalid ID format, must match expression: %s", partialIDRegex.String())
	}

	*id = Partial{
		raw:    matches[0],
		Vendor: matches[1],
		Name:   matches[2],
	}
	return nil
}

func (id *Partial) MarshalText() ([]byte, error) {
	if id.raw == "" {
		return nil, errors.New("Cannot marshal uninitialized ID")
	}
	return []byte(id.raw), nil
}
