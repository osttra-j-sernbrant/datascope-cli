package main

import (
	"datascope-cli/pkg/datascope"
	"testing"
)

func TestParseIdentifier(t *testing.T) {
	tests := []struct {
		name           string
		arg            string
		wantIdentifier string
		wantType       string
		wantSource     string
	}{
		{
			name:           "default type",
			arg:            "AAPL.O",
			wantIdentifier: "AAPL.O",
			wantType:       "Ric",
			wantSource:     "",
		},
		{
			name:           "explicit type",
			arg:            "Cusip:037833100",
			wantIdentifier: "037833100",
			wantType:       "Cusip",
			wantSource:     "",
		},
		{
			name:           "type and source",
			arg:            "Cusip:037833100:NYS",
			wantIdentifier: "037833100",
			wantType:       "Cusip",
			wantSource:     "NYS",
		},
		{
			name:           "empty arg",
			arg:            "",
			wantIdentifier: "",
			wantType:       "Ric",
			wantSource:     "",
		},
		{
			name:           "multiple colons (joined in source)",
			arg:            "isin:ID:Source:Extra",
			wantIdentifier: "ID",
			wantType:       "Isin",
			wantSource:     "Source:Extra",
		},
		{
			name:           "case insensitive type",
			arg:            "isin:US0378331005",
			wantIdentifier: "US0378331005",
			wantType:       "Isin",
			wantSource:     "",
		},
		{
			name:           "uppercase type",
			arg:            "ISIN:US0378331005",
			wantIdentifier: "US0378331005",
			wantType:       "Isin",
			wantSource:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := datascope.ParseIdentifier(tt.arg)
			if id.Identifier != tt.wantIdentifier {
				t.Errorf("datascope.ParseIdentifier() gotIdentifier = %v, want %v", id.Identifier, tt.wantIdentifier)
			}
			if id.IdentifierType != tt.wantType {
				t.Errorf("datascope.ParseIdentifier() gotType = %v, want %v", id.IdentifierType, tt.wantType)
			}
			if id.Source != tt.wantSource {
				t.Errorf("datascope.ParseIdentifier() gotSource = %v, want %v", id.Source, tt.wantSource)
			}
		})
	}
}
