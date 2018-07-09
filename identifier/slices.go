package identifier

import "github.com/pkg/errors"

func ParseComposeds(ids []string) ([]Composed, error) {
	composedIDs := make([]Composed, len(ids))
	for i, id := range ids {
		composedID, err := ParseComposed(id)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to parse ID")
		}
		composedIDs[i] = composedID
	}

	return composedIDs, nil
}

func SerializeComposeds(composedIDs []Composed) []string {
	ids := make([]string, len(composedIDs))
	for i, composedID := range composedIDs {
		ids[i] = composedID.String()
	}

	return ids
}
