package util

import (
	"errors"
	"fmt"
)

type Integer interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		uintptr
}

var errConvert = errors.New("integer conversion failed")

func SafeConvert[N, M Integer](n N) (M, error) {
	if N(M(n)) != n {
		return 0, errConvert
	}
	return M(n), nil
}

var ErrOverflow = errors.New("number overflows int64")

func ParseInt64(b []byte) (int64, error) {
	n := int64(0)
	if len(b) == 0 {
		return -1, errors.New("empty number string")
	}
	isNegative := b[0] == '-'

	rangeBytes := b
	if isNegative {
		rangeBytes = b[1:]
	}
	for _, ch := range rangeBytes {
		if ch < '0' || ch > '9' {
			return -1, fmt.Errorf("invalid number %q", b)
		}

		lastN := n
		n *= 10
		overflowAfterMultiply := n < lastN
		n += int64(ch - '0')
		if overflowAfterMultiply || n < lastN {
			return -1, ErrOverflow
		}
	}

	if isNegative {
		// cannot represent math.MinInt64
		return n * -1, nil
	}
	return n, nil
}
