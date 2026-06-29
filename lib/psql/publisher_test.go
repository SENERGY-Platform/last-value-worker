/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package psql

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/last-value-worker/lib/log"
)

func TestFlatten(t *testing.T) {
	log.InitForTest()
	base := map[string]interface{}{
		"metrics": map[string]interface{}{
			"level":      42,
			"level_unit": "test2",
			"thisisatestforveryveryveryveryveryverylongfieldnameswhichneedtobehashed": 42,
			"title":      "event",
			"updateTime": 13,
			"listvariable": []map[string]interface{}{
				{"value": 12, "value2": 34},
				{"value": 56, "value2": 78},
				{"value": 90, "value2": 12},
			},
			"listfixed": []map[string]interface{}{
				{"value": 12, "value2": 34},
				{"value": 56, "value2": 78},
			},
			"listfixedsimple":       []interface{}{12, 34},
			"listfixedsimplestring": []interface{}{"12", "34"},
		},
		"other_var": "foo",
	}
	jsonBytes, err := json.Marshal(base)
	if err != nil {
		t.Error(err)
		return
	}
	var input map[string]interface{}
	err = json.Unmarshal(jsonBytes, &input)
	if err != nil {
		t.Error(err)
		return
	}
	expected := map[string]interface{}{
		"metrics.level":      42,
		"metrics.level_unit": "'test2'",
		"metrics.thisisatestforveryveryveryveryveryverylongfieldnameswhichneedtobehashed": 42,
		"metrics.title":                   "'event'",
		"metrics.updateTime":              13,
		"metrics.listvariable.0.value":    12,
		"metrics.listvariable.0.value2":   34,
		"metrics.listvariable.1.value":    56,
		"metrics.listvariable.1.value2":   78,
		"metrics.listvariable.2.value":    90,
		"metrics.listvariable.2.value2":   12,
		"metrics.listfixed.0.value":       12,
		"metrics.listfixed.0.value2":      34,
		"metrics.listfixed.1.value":       56,
		"metrics.listfixed.1.value2":      78,
		"metrics.listfixedsimple.0":       12,
		"metrics.listfixedsimple.1":       34,
		"metrics.listfixedsimplestring.0": "'12'",
		"metrics.listfixedsimplestring.1": "'34'",
		"other_var":                       "'foo'",
	}
	result, err := flatten("deviceId", input, nil, "")
	if err != nil {
		t.Error(err)
		return
	}
	if len(result) != len(expected) {
		t.Errorf("expected %d fields, got %d", len(expected), len(result))
		return
	}
	expectedStr, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	resultStr, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Error(err)
		return
	}
	if string(expectedStr) != string(resultStr) {
		t.Error("\n" + jsonDiff(expected, result))
	}
}

// helper for easy debugging
func jsonDiff(a, b map[string]interface{}) string {
	aj, _ := json.MarshalIndent(a, "", "  ")
	bj, _ := json.MarshalIndent(b, "", "  ")

	aLines := strings.Split(string(aj), "\n")
	bLines := strings.Split(string(bj), "\n")

	var diff strings.Builder
	maxLen := max(len(aLines), len(bLines))

	for i := range maxLen {
		aLine, bLine := "", ""
		if i < len(aLines) {
			aLine = aLines[i]
		}
		if i < len(bLines) {
			bLine = bLines[i]
		}

		if aLine != bLine {
			if aLine != "" {
				fmt.Fprintf(&diff, "- %s\n", aLine)
			}
			if bLine != "" {
				fmt.Fprintf(&diff, "+ %s\n", bLine)
			}
		}
	}

	return diff.String()
}
