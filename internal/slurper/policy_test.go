package slurper

import (
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/stretchr/testify/require"
)

func TestSigRequired(t *testing.T) {
	testCases := []struct {
		name          string
		override      *bool
		globalDefault bool
		want          bool
	}{
		{
			name:          "nil override uses global default true",
			override:      nil,
			globalDefault: true,
			want:          true,
		},
		{
			name:          "nil override uses global default false",
			override:      nil,
			globalDefault: false,
			want:          false,
		},
		{
			name:          "override true overrides global default false",
			override:      boolPtr(true),
			globalDefault: false,
			want:          true,
		},
		{
			name:          "override false overrides global default true",
			override:      boolPtr(false),
			globalDefault: true,
			want:          false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := SigRequired(tc.override, tc.globalDefault)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestKeepLabel(t *testing.T) {
	testCases := []struct {
		name        string
		sig         []byte
		sigRequired bool
		want        bool
	}{
		{
			name:        "signed label kept when sig required true",
			sig:         []byte{0x01, 0x02, 0x03},
			sigRequired: true,
			want:        true,
		},
		{
			name:        "signed label kept when sig required false",
			sig:         []byte{0x01, 0x02, 0x03},
			sigRequired: false,
			want:        true,
		},
		{
			name:        "unsigned label dropped when sig required true",
			sig:         nil,
			sigRequired: true,
			want:        false,
		},
		{
			name:        "unsigned label kept when sig required false",
			sig:         nil,
			sigRequired: false,
			want:        true,
		},
		{
			name:        "empty sig dropped when sig required true",
			sig:         []byte{},
			sigRequired: true,
			want:        false,
		},
		{
			name:        "empty sig kept when sig required false",
			sig:         []byte{},
			sigRequired: false,
			want:        true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			label := &comatproto.LabelDefs_Label{
				Sig: tc.sig,
			}
			got := KeepLabel(label, tc.sigRequired)
			require.Equal(t, tc.want, got)
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}
