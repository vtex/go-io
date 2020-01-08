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
	raw := string(b)
	matches := strings.Split(raw, ".")
	if len(matches) == 0 {
		return errors.Errorf("Invalid partial ID format, must be vendor.name")
	}

	*id = Partial{
		raw:    raw,
		Vendor: matches[0],
		Name:   matches[1],
	}
	return nil
}

func (id *Partial) MarshalText() ([]byte, error) {
	if id.raw == "" {
		return nil, errors.New("Cannot marshal uninitialized ID")
	}
	return []byte(id.raw), nil
}
