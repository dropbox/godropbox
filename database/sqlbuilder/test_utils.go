package sqlbuilder

var table1Col1 = IntColumnWithIsPrimaryKey("col1", Nullable, NotPrimaryKey)
var table1Col2 = IntColumnWithIsPrimaryKey("col2", Nullable, NotPrimaryKey)
var table1Col3 = IntColumnWithIsPrimaryKey("col3", Nullable, NotPrimaryKey)
var table1Col4 = DateTimeColumnWithIsPrimaryKey("col4", Nullable, NotPrimaryKey)
var table1 = NewTable(
	"table1",
	table1Col1,
	table1Col2,
	table1Col3,
	table1Col4)

var table2Col3 = IntColumnWithIsPrimaryKey("col3", Nullable, NotPrimaryKey)
var table2Col4 = IntColumnWithIsPrimaryKey("col4", Nullable, NotPrimaryKey)
var table2 = NewTable(
	"table2",
	table2Col3,
	table2Col4)

var table3Col1 = IntColumnWithIsPrimaryKey("col1", Nullable, NotPrimaryKey)
var table3Col2 = IntColumnWithIsPrimaryKey("col2", Nullable, NotPrimaryKey)
var table3 = NewTable(
	"table3",
	table3Col1,
	table3Col2)
