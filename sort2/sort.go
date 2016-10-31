package sort2

import (
	"bytes"
	"sort"
	"time"
)

//
// UintSlice ----------------------------------------------------------------
//

type UintSlice []uint

func (s UintSlice) Len() int {
	return len(s)
}

func (s UintSlice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s UintSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s UintSlice) Sort() {
	sort.Sort(s)
}

func Uints(s []uint) {
	sort.Sort(UintSlice(s))
}

//
// Uint64Slice ----------------------------------------------------------------
//

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Sort() {
	sort.Sort(s)
}

func Uint64s(s []uint64) {
	sort.Sort(Uint64Slice(s))
}

//
// Uint32Slice ----------------------------------------------------------------
//

type Uint32Slice []uint32

func (s Uint32Slice) Len() int {
	return len(s)
}

func (s Uint32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint32Slice) Sort() {
	sort.Sort(s)
}

func Uint32s(s []uint32) {
	sort.Sort(Uint32Slice(s))
}

//
// Uint16Slice ----------------------------------------------------------------
//

type Uint16Slice []uint16

func (s Uint16Slice) Len() int {
	return len(s)
}

func (s Uint16Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint16Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint16Slice) Sort() {
	sort.Sort(s)
}

func Uint16s(s []uint16) {
	sort.Sort(Uint16Slice(s))
}

//
// Uint8Slice ----------------------------------------------------------------
//

type Uint8Slice []uint8

func (s Uint8Slice) Len() int {
	return len(s)
}

func (s Uint8Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Uint8Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint8Slice) Sort() {
	sort.Sort(s)
}

func Uint8s(s []uint8) {
	sort.Sort(Uint8Slice(s))
}

//
// Int64Slice ----------------------------------------------------------------
//

type Int64Slice []int64

func (s Int64Slice) Len() int {
	return len(s)
}

func (s Int64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Int64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Int64Slice) Sort() {
	sort.Sort(s)
}

func Int64s(s []int64) {
	sort.Sort(Int64Slice(s))
}

//
// Int32Slice ----------------------------------------------------------------
//

type Int32Slice []int32

func (s Int32Slice) Len() int {
	return len(s)
}

func (s Int32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Int32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Int32Slice) Sort() {
	sort.Sort(s)
}

func Int32s(s []int32) {
	sort.Sort(Int32Slice(s))
}

//
// Int16Slice ----------------------------------------------------------------
//

type Int16Slice []int16

func (s Int16Slice) Len() int {
	return len(s)
}

func (s Int16Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Int16Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Int16Slice) Sort() {
	sort.Sort(s)
}

func Int16s(s []int16) {
	sort.Sort(Int16Slice(s))
}

//
// Int8Slice ----------------------------------------------------------------
//

type Int8Slice []int8

func (s Int8Slice) Len() int {
	return len(s)
}

func (s Int8Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Int8Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Int8Slice) Sort() {
	sort.Sort(s)
}

func Int8s(s []int8) {
	sort.Sort(Int8Slice(s))
}

//
// Float32Slice ----------------------------------------------------------------
//

type Float32Slice []float32

func (s Float32Slice) Len() int {
	return len(s)
}

func (s Float32Slice) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s Float32Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Float32Slice) Sort() {
	sort.Sort(s)
}

func Float32s(s []float32) {
	sort.Sort(Float32Slice(s))
}

//
// ByteArraySlice -------------------------------------------------------------
//

type ByteArraySlice [][]byte

func (s ByteArraySlice) Len() int {
	return len(s)
}

func (s ByteArraySlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

func (s ByteArraySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s ByteArraySlice) Sort() {
	sort.Sort(s)
}

func ByteArrays(s [][]byte) {
	sort.Sort(ByteArraySlice(s))
}

//
// TimeSlice ----------------------------------------------------------------
//

type TimeSlice []time.Time

func (s TimeSlice) Len() int {
	return len(s)
}

func (s TimeSlice) Less(i, j int) bool {
	return s[i].Before(s[j])
}

func (s TimeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s TimeSlice) Sort() {
	sort.Sort(s)
}

func Times(s []time.Time) {
	sort.Sort(TimeSlice(s))
}
