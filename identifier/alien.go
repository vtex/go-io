package identifier

import (
	"strings"

	"github.com/pkg/errors"
)

type Alien struct {
	raw     string
	Engine  string
	Package string
	Version string
}

// Assert Alien implements Composed interface.
var _ Composed = (*Alien)(nil)

func NewAlien(engine, pkg, version string) *Alien {
	return &Alien{
		raw:     engine + ":" + pkg + "@" + version,
		Engine:  engine,
		Package: pkg,
		Version: version,
	}
}

func (id *Alien) String() string {
	return id.raw
}

func (id *Alien) Matches(other ID) bool {
	return id.compare(other) == 0
}

func (id *Alien) LessThan(other ID) bool {
	return id.compare(other) < 0
}

func (id *Alien) compare(other ID) int {
	return strings.Compare(id.String(), other.String())
}

func (id *Alien) Prefix() string {
	return id.Engine + ":" + id.Package
}

func (id *Alien) Suffix() string {
	return id.Version
}

func (id *Alien) MarshalText() ([]byte, error) {
	if id.raw == "" {
		return nil, errors.New("Cannot marshal uninitialized ID")
	}
	return []byte(id.raw), nil
}
