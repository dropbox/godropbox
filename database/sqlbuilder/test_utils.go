package sqlbuilder

var table1Col1 = IntColumn("col1", true)
var table1Col2 = IntColumn("col2", true)
var table1Col3 = IntColumn("col3", true)
var table1 = NewTable(
	"table1",
	table1Col1,
	table1Col2,
	table1Col3)

var table2Col3 = IntColumn("col3", true)
var table2Col4 = IntColumn("col4", true)
var table2 = NewTable(
	"table2",
	table2Col3,
	table2Col4)

var table3Col1 = IntColumn("col1", true)
var table3Col2 = IntColumn("col2", true)
var table3 = NewTable(
	"table3",
	table3Col1,
	table3Col2)
