package sqlbuilder_test

import (
	"fmt"

	sb "github.com/dropbox/godropbox/database/sqlbuilder"
)

func Example() {
	t1 := sb.NewTable(
		"parent_prefix",
		sb.IntColumn("ns_id", sb.NotNullable),
		sb.IntColumn("hash", sb.NotNullable),
		sb.StrColumn(
			"prefix",
			sb.UTF8,
			sb.UTF8CaseInsensitive,
			sb.NotNullable))

	t2 := sb.NewTable(
		"sfj",
		sb.IntColumn("ns_id", sb.NotNullable),
		sb.IntColumn("sjid", sb.NotNullable),
		sb.StrColumn(
			"filename",
			sb.UTF8,
			sb.UTF8CaseInsensitive,
			sb.NotNullable))

	ns_id1 := t1.C("ns_id")
	prefix := t1.C("prefix")
	ns_id2 := t2.C("ns_id")
	sjid := t2.C("sjid")
	filename := t2.C("filename")

	in := []int32{1, 2, 3}
	join := t2.LeftJoinOn(t1, sb.Eq(ns_id1, ns_id2))
	q := join.Select(ns_id2, sjid, prefix, filename).Where(
		sb.And(sb.EqL(ns_id2, 123), sb.In(sjid, in)))
	fmt.Println(q.String("shard1"))
}
