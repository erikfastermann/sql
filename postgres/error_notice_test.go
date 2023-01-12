package postgres

import "testing"

var BenchmarkErrorAndNoticeFieldsStringResult string

func BenchmarkErrorAndNoticeFieldsString(b *testing.B) {
	var e errorAndNoticeFields
	e.severity = "Foo"
	e.message = "Bar"
	for i := 0; i < b.N; i++ {
		BenchmarkErrorAndNoticeFieldsStringResult = e.String()
	}
}
