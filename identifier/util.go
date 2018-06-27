package identifier

import (
	"bytes"
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
