package bytes2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMax(t *testing.T) {
	require.Equal(t, []byte{172}, Max([]byte{172}, []byte{171}))
	require.Equal(t, []byte{172}, Max([]byte{171}, []byte{172}))

	require.Equal(t, []byte{172, 200}, Max([]byte{172}, []byte{172, 200}))
	require.Equal(t, []byte{172, 200}, Max([]byte{172, 200}, []byte{172}))

	require.Equal(t, []byte{172, 201}, Max([]byte{172, 201}, []byte{172, 200}))
	require.Equal(t, []byte{172, 201}, Max([]byte{172, 200}, []byte{172, 201}))
}

func TestMin(t *testing.T) {
	require.Equal(t, []byte{171}, Min([]byte{172}, []byte{171}))
	require.Equal(t, []byte{171}, Min([]byte{171}, []byte{172}))

	require.Equal(t, []byte{172}, Min([]byte{172}, []byte{172, 200}))
	require.Equal(t, []byte{172}, Min([]byte{172, 200}, []byte{172}))

	require.Equal(t, []byte{172, 200}, Min([]byte{172, 201}, []byte{172, 200}))
	require.Equal(t, []byte{172, 200}, Min([]byte{172, 200}, []byte{172, 201}))
}
