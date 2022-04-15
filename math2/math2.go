package math2

// Copies of functions in the math stdlib package for types other than float64.

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MaxInt(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MaxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MaxUint64(a, b uint64) uint64 {
	if a < b {
		return b
	}
	return a
}

func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

func MaxUint32(a, b uint32) uint32 {
	if a < b {
		return b
	}
	return a
}
