package identifier

import (
	"strings"
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
	}
	return 1
}

func (id *Partial) AtVersion(version string) (*Version, error) {
	return NewVersion(id.Vendor, id.Name, version)
}

func (id *Partial) WithRange(constraint string) (*Range, error) {
	return NewRange(id.Vendor, id.Name, constraint)
}
