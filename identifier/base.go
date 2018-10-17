package identifier

type MatchOptions struct {
	IgnorePrerelease bool
}

// Base interface for all types of identifiers.
type ID interface {
	String() string
	Matches(other ID) bool
	LessThan(other ID) bool

	compare(other ID) int
}

// Identifier composed by 2 parts separated by an '@'. This
// normally means the name of the app on the prefix and the
// version specification in the suffix.
type Composed interface {
	ID
	MatchesOpt(other ID, opts MatchOptions) bool
	Prefix() string
	Suffix() string
}

// Composed identifier that actually refers to an I/O app,
// meaning it has a proper vendor and name, and can be
// converted to a partial app id.
type Qualified interface {
	Composed
	Vendor() string
	Name() string
	ToPartial() *Partial
}
