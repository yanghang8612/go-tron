package tronapi

import (
	"encoding/json"
	"testing"
)

func TestResourceFieldUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    ResourceField
		wantErr bool
	}{
		{
			name:    "number 0 (BANDWIDTH)",
			input:   "0",
			want:    0,
			wantErr: false,
		},
		{
			name:    "number 1 (ENERGY)",
			input:   "1",
			want:    1,
			wantErr: false,
		},
		{
			name:    "number 2 (TRON_POWER)",
			input:   "2",
			want:    2,
			wantErr: false,
		},
		{
			name:    "string BANDWIDTH",
			input:   `"BANDWIDTH"`,
			want:    0,
			wantErr: false,
		},
		{
			name:    "string ENERGY",
			input:   `"ENERGY"`,
			want:    1,
			wantErr: false,
		},
		{
			name:    "string TRON_POWER",
			input:   `"TRON_POWER"`,
			want:    2,
			wantErr: false,
		},
		{
			name:    "unknown string",
			input:   `"UNKNOWN"`,
			want:    0,
			wantErr: true,
		},
		{
			name:    "invalid number (too large)",
			input:   "9999999999999999999999",
			want:    0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r ResourceField
			err := json.Unmarshal([]byte(tt.input), &r)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && r != tt.want {
				t.Errorf("UnmarshalJSON() got %d, want %d", r, tt.want)
			}
		})
	}
}
