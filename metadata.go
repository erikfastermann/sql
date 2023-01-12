package main

type metadata struct {
	parameterOids []int
	fields        []field
}

type field struct {
	name string

	maybeTableOid              int
	maybeColumnAttributeNumber int

	typeOid      int
	typeSize     int
	typeModifier int

	formatCode int
}
