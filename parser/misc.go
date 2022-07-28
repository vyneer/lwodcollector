package parser

import (
	"database/sql"

	"google.golang.org/api/sheets/v4"
)

func fillWithBlank(v *[]*sheets.CellData, maxValueOfTemplate int64) {
	if len(*v) < int(maxValueOfTemplate)+1 {
		for i := len(*v); i < int(maxValueOfTemplate)+1; i++ {
			blank := sheets.CellData{
				FormattedValue: "",
			}
			*v = append(*v, &blank)
		}
	}
}

func getRowValues(row []*sheets.CellData) []string {
	var rowStrings []string

	for _, v := range row {
		rowStrings = append(rowStrings, v.FormattedValue)
	}

	return rowStrings
}

// https://stackoverflow.com/a/40268372
func newNullString(s string) sql.NullString {
	if len(s) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{
		String: s,
		Valid:  true,
	}
}

func dedupHashes(m map[string]string, e map[string][]LWODEntry) map[string][]LWODEntry {
	dedupedHashes := make(map[string]string)

	for key, valueOuter := range m {
		exists := false
		for _, valueInner := range dedupedHashes {
			if valueInner == valueOuter {
				exists = true
			}
		}
		if !exists {
			dedupedHashes[key] = valueOuter
		}
	}

	dedupedEntries := make(map[string][]LWODEntry)
	for key := range dedupedHashes {
		dedupedEntries[key] = e[key]
	}

	return dedupedEntries
}
