package sort2

import (
	"bytes"
	"sort"
	"time"
)

//
// UintSlice ----------------------------------------------------------------
//

// UintSlice represents a slice that holds uint elements.
type UintSlice []uint

// Len returns the length of the UintSlice instance.
func (s UintSlice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the UintSlice.
func (s UintSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the UintSlice.
func (s UintSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the UintSlice.
func (s UintSlice) Sort() {
	sort.Sort(s)
}

// Uints takes a uint slice as a parameter and sorts it.
func Uints(s []uint) {
	sort.Sort(UintSlice(s))
}

//
// Uint64Slice ----------------------------------------------------------------
//

// Uint64Slice represents a slice that holds uint64 elements.
type Uint64Slice []uint64

// Len returns the length of the Uint64Slice instance.
func (s Uint64Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Uint64Slice.
func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Uint64Slice.
func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Uint64Slice.
func (s Uint64Slice) Sort() {
	sort.Sort(s)
}

// Uint64s takes a uint64 slice as a parameter and sorts it.
func Uint64s(s []uint64) {
	sort.Sort(Uint64Slice(s))
}

//
// Uint32Slice ----------------------------------------------------------------
//

// Uint32Slice represents a slice that holds uint32 elements.
type Uint32Slice []uint32

// Len returns the length of the Uint32Slice instance.
func (s Uint32Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Uint32Slice.
func (s Uint32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Uint32Slice.
func (s Uint32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Uint32Slice.
func (s Uint32Slice) Sort() {
	sort.Sort(s)
}

// Uint32s takes a uint32 slice as a parameter and sorts it.
func Uint32s(s []uint32) {
	sort.Sort(Uint32Slice(s))
}

//
// Uint16Slice ----------------------------------------------------------------
//

// Uint16Slice represents a slice that holds uint16 elements.
type Uint16Slice []uint16

// Len returns the length of the Uint16Slice instance.
func (s Uint16Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Uint16Slice.
func (s Uint16Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Uint16Slice.
func (s Uint16Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Uint16Slice.
func (s Uint16Slice) Sort() {
	sort.Sort(s)
}

// Uint16s takes a uint16 slice as a parameter and sorts it.
func Uint16s(s []uint16) {
	sort.Sort(Uint16Slice(s))
}

//
// Uint8Slice ----------------------------------------------------------------
//

// Uint8Slice represents a slice that holds uint8 elements.
type Uint8Slice []uint8

// Len returns the length of the Uint8Slice instance.
func (s Uint8Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Uint8Slice.
func (s Uint8Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Uint8Slice
func (s Uint8Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Uint8Slice
func (s Uint8Slice) Sort() {
	sort.Sort(s)
}

// Uint8s takes a uint16 slice as a parameter and sorts it.
func Uint8s(s []uint8) {
	sort.Sort(Uint8Slice(s))
}

//
// Int64Slice ----------------------------------------------------------------
//

// Int64Slice represents a slice that holds int64 elements.
type Int64Slice []int64

// Len returns the length of the Int64Slice instance.
func (s Int64Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Int64Slice.
func (s Int64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Int64Slice
func (s Int64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Int64Slice
func (s Int64Slice) Sort() {
	sort.Sort(s)
}

// Int64s takes an int64 slice as a parameter and sorts it.
func Int64s(s []int64) {
	sort.Sort(Int64Slice(s))
}

//
// Int32Slice ----------------------------------------------------------------
//

// Int32Slice represents a slice that holds int32 elements.
type Int32Slice []int32

// Len returns the length of the Int32Slice instance.
func (s Int32Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Int32Slice.
func (s Int32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Int32Slice
func (s Int32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Int32Slice.
func (s Int32Slice) Sort() {
	sort.Sort(s)
}

// Int32s takes an int32 slice as a parameter and sorts it.
func Int32s(s []int32) {
	sort.Sort(Int32Slice(s))
}

//
// Int16Slice ----------------------------------------------------------------
//

// Int16Slice represents a slice that holds int16 elements.
type Int16Slice []int16

// Len returns the length of the Int16Slice instance.
func (s Int16Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Int16Slice.
func (s Int16Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Int16Slice.
func (s Int16Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Int16Slice.
func (s Int16Slice) Sort() {
	sort.Sort(s)
}

// Int16s takes an int16 slice as a parameter and sorts it.
func Int16s(s []int16) {
	sort.Sort(Int16Slice(s))
}

//
// Int8Slice ----------------------------------------------------------------
//

// Int8Slice represents a slice that holds int8 elements.
type Int8Slice []int8

// Len returns the length of the Int8Slice instance.
func (s Int8Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Int8Slice.
func (s Int8Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Int8Slice.
func (s Int8Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Int8Slice.
func (s Int8Slice) Sort() {
	sort.Sort(s)
}

// Int8s takes an int8 slice as a parameter and sorts it.
func Int8s(s []int8) {
	sort.Sort(Int8Slice(s))
}

//
// Float32Slice ----------------------------------------------------------------
//

// Float32Slice represents a slice that holds float32 elements.
type Float32Slice []float32

// Len returns the length of the Float32Slice instance.
func (s Float32Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Float32Slice.
func (s Float32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Float32Slice.
func (s Float32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Float32Slice.
func (s Float32Slice) Sort() {
	sort.Sort(s)
}

// Float32s takes an float32 slice as a parameter and sorts it.
func Float32s(s []float32) {
	sort.Sort(Float32Slice(s))
}

//
// Float64Slice ----------------------------------------------------------------
//

// Float64Slice represents a slice that holds float64 elements.
type Float64Slice []float64

// Len returns the length of the Float64Slice instance.
func (s Float64Slice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the Float64Slice.
func (s Float64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

// Swap swaps the positions of the elements indices i and j of the Float64Slice.
func (s Float64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the Float64Slice.
func (s Float64Slice) Sort() {
	sort.Sort(s)
}

// Float64s takes an float64 slice as a parameter and sorts it.
func Float64s(s []float64) {
	sort.Sort(Float64Slice(s))
}

//
// ByteArraySlice -------------------------------------------------------------
//

// ByteArraySlice represents a slice that holds byte arrays.
type ByteArraySlice [][]byte

// Len returns the length of the ByteArraySlice instance.
func (s ByteArraySlice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the ByteArraySlice.
func (s ByteArraySlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

// Swap swaps the positions of the elements indices i and j of the ByteArraySlice.
func (s ByteArraySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the ByteArraySlice.
func (s ByteArraySlice) Sort() {
	sort.Sort(s)
}

// ByteArrays takes a slice of byte slices as a parameter and sorts it.
func ByteArrays(s [][]byte) {
	sort.Sort(ByteArraySlice(s))
}

//
// TimeSlice ----------------------------------------------------------------
//

// TimeSlice represents a slice that holds elements of the type time.Time.
type TimeSlice []time.Time

// Len returns the length of the TimeSlice instance.
func (s TimeSlice) Len() int {
	return len(s)
}

// Less reports whether the element with
// index i should sort before the element with index j in the TimeSlice.
func (s TimeSlice) Less(i, j int) bool {
	return s[i].Before(s[j])
}

// Swap swaps the positions of the elements indices i and j of the TimeSlice.
func (s TimeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort sorts the elements within the TimeSlice.
func (s TimeSlice) Sort() {
	sort.Sort(s)
}

// Times takes a slice of time elements as a parameter and sorts it.
func Times(s []time.Time) {
	sort.Sort(TimeSlice(s))
}
