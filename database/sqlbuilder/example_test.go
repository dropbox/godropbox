package sqlbuilder

import "fmt"

func Example() {
	t1 := NewTable(
		"parent_prefix",
		IntColumn("ns_id", NotNullable),
		IntColumn("hash", NotNullable),
		StrColumn("prefix",
			UTF8,
			UTF8CaseInsensitive,
			NotNullable))

	t2 := NewTable(
		"sfj",
		IntColumn("ns_id", NotNullable),
		IntColumn("sjid", NotNullable),
		StrColumn("filename",
			UTF8,
			UTF8CaseInsensitive,
			NotNullable))

	ns_id1 := t1.C("ns_id")
	prefix := t1.C("prefix")
	ns_id2 := t2.C("ns_id")
	sjid := t2.C("sjid")
	filename := t2.C("filename")

	in := []int32{1, 2, 3}
	join := t2.LeftJoinOn(t1, Eq(ns_id1, ns_id2))
	q := join.Select(ns_id2, sjid, prefix, filename).Where(
		And(EqL(ns_id2, 456), In(sjid, in)))
	text, _ := q.String("shard1")
	fmt.Println(text)
	// Output:
	// SELECT `sfj`.`ns_id`,`sfj`.`sjid`,`parent_prefix`.`prefix`,`sfj`.`filename` FROM `shard1`.`sfj` LEFT JOIN `shard1`.`parent_prefix` ON `parent_prefix`.`ns_id`=`sfj`.`ns_id` WHERE (`sfj`.`ns_id`=456 AND `sfj`.`sjid` IN (1,2,3))
}
