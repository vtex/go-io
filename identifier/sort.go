package identifier

func LessVersion(ids []*Version) func(int, int) bool {
	return func(i, j int) bool {
		return ids[i].LessThan(ids[j])
	}
}

func LessRange(ids []*Range) func(int, int) bool {
	return func(i, j int) bool {
		return ids[i].LessThan(ids[j])
	}
}

func LessPartial(ids []*Partial) func(int, int) bool {
	return func(i, j int) bool {
		return ids[i].LessThan(ids[j])
	}
}

func LessAlien(ids []*Alien) func(int, int) bool {
	return func(i, j int) bool {
		return ids[i].LessThan(ids[j])
	}
}

func LessComposed(ids []Composed) func(int, int) bool {
	return func(i, j int) bool {
		// Compare String representations as LessThan is not well-defined between different ID types.
		return ids[i].String() < ids[j].String()
	}
}

func Reverse(less func(int, int) bool) func(int, int) bool {
	return func(i, j int) bool {
		return less(j, i)
	}
}
