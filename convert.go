package main

import "errors"

type integer interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		uintptr
}

var errConvert = errors.New("integer conversion failed")

func safeConvert[N, M integer](n N) (M, error) {
	if N(M(n)) != n {
		return 0, errConvert
	}
	return M(n), nil
}
