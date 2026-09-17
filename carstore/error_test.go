package carstore

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnusableErrWrap(t *testing.T) {
	inner := io.ErrUnexpectedEOF
	err := fmt.Errorf("%w: cannot read advertisement data: %w", ErrUnusableCannotReadAd, inner)

	got, ok := errors.AsType[ErrUnusable](err)
	require.True(t, ok)
	require.Equal(t, ErrUnusableCannotReadAd, got)
	require.ErrorIs(t, err, ErrUnusableCannotReadAd)
	require.ErrorIs(t, err, inner)

	got, ok = errors.AsType[ErrUnusable](fmt.Errorf("read failed: %w", err))
	require.True(t, ok)
	require.Equal(t, ErrUnusableCannotReadAd, got)
	require.Contains(t, err.Error(), "CAR file is unusable")
	require.Contains(t, err.Error(), "cannot read advertisement data")
}

func TestUnusableErrNotUnusable(t *testing.T) {
	_, ok := errors.AsType[ErrUnusable](errors.New("not a car error"))
	require.False(t, ok)
	_, ok = errors.AsType[ErrUnusable](nil)
	require.False(t, ok)
}
