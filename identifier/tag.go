package identifier

import "strings"

type Tag struct {
	raw    string
	vendor string
	name   string
	Tag    string
}

// Assert Tag implements Qualified ID interface.
var _ Qualified = (*Tag)(nil)

func NewTag(vendor, name, tag string) *Tag {
	return &Tag{
		raw:    vendor + "." + name + "@" + tag,
		vendor: vendor,
		name:   name,
		Tag:    tag,
	}
}

func (id *Tag) String() string {
	return id.raw
}

func (id *Tag) Matches(other ID) bool {
	return id.compare(other) == 0
}

func (id *Tag) LessThan(other ID) bool {
	return id.compare(other) < 0
}

func (id *Tag) compare(other ID) int {
	switch other := other.(type) {
	case *Tag:
		return strings.Compare(id.raw, other.raw)
	case Qualified:
		return strings.Compare(id.Prefix(), other.Prefix())
	}
	return 1
}

func (id *Tag) Prefix() string {
	return id.vendor + "." + id.name
}

func (id *Tag) Suffix() string {
	return id.Tag
}

func (id *Tag) Vendor() string {
	return id.vendor
}

func (id *Tag) Name() string {
	return id.name
}

func (id *Tag) ToPartial() *Partial {
	return NewPartial(id.vendor, id.name)
}
