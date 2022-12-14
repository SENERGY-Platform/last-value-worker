/*
 * Copyright 2022 InfAI (CC SES)
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

package meta

import (
	"log"
	"sort"
)

func ParseContentVariable(c ContentVariable, path string) []string {
	s := []string{}
	prefix := path
	if len(prefix) > 0 {
		prefix += "."
	}
	prefix += c.Name
	switch c.Type {
	case String:
		s = append(s, prefix)
	case Boolean:
		s = append(s, prefix)
	case Float:
		s = append(s, prefix)
	case Integer:
		s = append(s, prefix)
	case Structure:
		sort.SliceStable(c.SubContentVariables, func(i, j int) bool { // guarantees same results for different orders
			return c.SubContentVariables[i].Id < c.SubContentVariables[j].Id
		})
		for _, sub := range c.SubContentVariables {
			s = append(s, ParseContentVariable(sub, prefix)...)
		}
	case List:
		log.Println("WARN: creating fields for list type not supported yet, skipping!")
	}
	return s
}

func EqualStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a) != len(b) {
		return false
	}
	for _, aElem := range a {
		if !elemInSlice(aElem, b) {
			return false
		}
	}
	return true
}

func elemInSlice(elem string, slice []string) bool {
	for _, sliceElem := range slice {
		if elem == sliceElem {
			return true
		}
	}
	return false
}

func GetDeepContentVariable(root ContentVariable, path []string) *ContentVariable {
	if len(path) == 0 {
		return &root
	}
	if root.SubContentVariables == nil {
		return nil
	}
	for _, sub := range root.SubContentVariables {
		if sub.Name == path[0] {
			return GetDeepContentVariable(sub, path[1:])
		}
	}
	return nil
}

func GetDeepValue(root interface{}, path []string) interface{} {
	if len(path) == 0 {
		return root
	}
	rootM, ok := root.(map[string]interface{})
	if !ok {
		return nil
	}
	sub, ok := rootM[path[0]]
	if !ok {
		return nil
	}
	return GetDeepValue(sub, path[1:])
}
