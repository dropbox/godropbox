package array2

// Return the first smallest or largest element for Min or Max respectively.

func MinInt(vals ...int) int {
	min := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] < min {
			min = vals[i]
		}
	}
	return min
}

func MaxInt(vals ...int) int {
	max := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] > max {
			max = vals[i]
		}
	}
	return max
}

func MinInt64(vals ...int64) int64 {
	min := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] < min {
			min = vals[i]
		}
	}
	return min
}

func MaxInt64(vals ...int64) int64 {
	max := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] > max {
			max = vals[i]
		}
	}
	return max
}

func MinUint64(vals ...uint64) uint64 {
	min := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] < min {
			min = vals[i]
		}
	}
	return min
}

func MaxUint64(vals ...uint64) uint64 {
	max := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] > max {
			max = vals[i]
		}
	}
	return max
}

func MinUint32(vals ...uint32) uint32 {
	min := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] < min {
			min = vals[i]
		}
	}
	return min
}

func MaxUint32(vals ...uint32) uint32 {
	max := vals[0]
	for i := 1; i < len(vals); i++ {
		if vals[i] > max {
			max = vals[i]
		}
	}
	return max
}
