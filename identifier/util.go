package identifier

import (
	"bytes"

	"github.com/Masterminds/semver"
)

func Join(ids []Composed, separator string) string {
	var buffer bytes.Buffer
	for i := range ids {
		id := ids[i]
		if _, ok := id.(Qualified); ok {
			if buffer.Len() > 0 {
				buffer.WriteString(separator)
			}
			buffer.WriteString(ids[i].String())
		}
	}

	return buffer.String()
}

func StripPrerelease(v *semver.Version) *semver.Version {
	// This will never fail as the only error would be a bad Prerelease.
	copy, _ := v.SetPrerelease("")
	return &copy
}
