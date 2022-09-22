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
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

func GetService(id string, url string) (Service, error) {
	url += "/services/" + id
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return Service{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return Service{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Service{}, errors.New("unexpected status code while getting service: " + strconv.Itoa(resp.StatusCode) + ", URL was " + url)
	}
	var service Service
	err = json.NewDecoder(resp.Body).Decode(&service)
	if err != nil {
		return Service{}, err
	}
	return service, nil
}
