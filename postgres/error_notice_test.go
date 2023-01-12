package postgres

import "testing"

var BenchmarkErrorAndNoticeFieldsStringResult string

func BenchmarkErrorAndNoticeFieldsString(b *testing.B) {
	var e ErrorAndNoticeFields
	e.Severity = "Foo"
	e.Message = "Bar"
	for i := 0; i < b.N; i++ {
		BenchmarkErrorAndNoticeFieldsStringResult = e.String()
	}
}
