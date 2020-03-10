// Copyright 2020 The Amadeus Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkaconnectclient

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog"
)

var httpClient = http.Client{Timeout: 5 * time.Minute}

// KCClientItf inter face for client
type KCClientItf interface {
	GetKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (map[string]interface{}, error)
	GetKafkaConnectorRunningTaskNb(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (int32, error)
	PutKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName, body []byte) error
	GetAllConnectors(kafkaConenctNamespacedname types.NamespacedName, port int32) ([]string, error)
	DeleteConnector(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) error
}

// KCClient client to acess to kafka connect rest api
type KCClient struct {
}

//DeleteConnector delete an connector via rest api
func (kcc *KCClient) DeleteConnector(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) error {
	httpClient := &http.Client{}
	url := fmt.Sprintf("http://%s.%s:%d/connectors/%s", fmt.Sprintf("%s-service", kafkaConenctNamespacedname.Name), kafkaConenctNamespacedname.Namespace, port, connectorName)
	//url := fmt.Sprintf("http://%s.apps-crc.testing/connectors/%s", kafkaConenctNamespacedname.Name, connectorName)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}

	response, err := httpClient.Do(req)
	if response.StatusCode > 399 {
		return fmt.Errorf("rest api return error, code : %d", response.StatusCode)
	}
	if err != nil {
		klog.Error(err, "error when getting current kafkaconnector config")
		return err
	}
	return nil
}

//GetKafkaConnectConfig get the existing config from kafkaconnect rest api
func (kcc *KCClient) GetKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s.%s:%d/connectors/%s/config", fmt.Sprintf("%s-service", kafkaConenctNamespacedname.Name), kafkaConenctNamespacedname.Namespace, port, connectorName)
	//url := fmt.Sprintf("http://%s.apps-crc.testing/connectors/%s/config", kafkaConenctNamespacedname.Name, connectorName)
	output := &map[string]interface{}{}
	response, err := httpClient.Get(url)
	if response.StatusCode == 404 {
		return make(map[string]interface{}), nil
	}
	if err != nil {
		klog.Error(err, "error when getting current kafkaconnector config")
		return nil, err
	}
	data, _ := ioutil.ReadAll(response.Body)

	if err = json.Unmarshal(data, output); err != nil {
		return nil, err
	}
	return *output, nil

}

//GetKafkaConnectorRunningTaskNb get the current running task number for a connector
func (kcc *KCClient) GetKafkaConnectorRunningTaskNb(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (int32, error) {
	url := fmt.Sprintf("http://%s.%s:%d/connectors/%s/status", fmt.Sprintf("%s-service", kafkaConenctNamespacedname.Name), kafkaConenctNamespacedname.Namespace, port, connectorName)
	//url := fmt.Sprintf("http://%s.apps-crc.testing/connectors/%s/status", kafkaConenctNamespacedname.Name, connectorName)
	output := &map[string]interface{}{}
	response, err := httpClient.Get(url)
	if err != nil {
		klog.Error(err, "error when getting current kafkaconnector config")
		return -1, err
	}
	data, _ := ioutil.ReadAll(response.Body)

	if err = json.Unmarshal(data, output); err != nil {
		return -1, err
	}

	nb := int32(0)
	if tasksItf, ok := (*output)["tasks"]; ok {
		if tasks, ok := tasksItf.([]interface{}); ok {

			for _, taskMap := range tasks {
				if task, ok := taskMap.(map[string]interface{}); ok {
					if state, ok := task["state"]; ok {
						if state == "RUNNING" {
							nb = nb + 1
						}
					}
				}

			}
			return nb, nil
		}
	}
	return -1, errors.New("cannot find task in status")

}

//GetConfigFromURL read the config from a url
func GetConfigFromURL(url string) (map[string]interface{}, error) {
	response, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	data, _ := ioutil.ReadAll(response.Body)

	config := &map[string]interface{}{}
	//set map with the byte
	if err = json.Unmarshal(data, config); err != nil {
		return nil, err
	}

	return *config, nil

}

//PutKafkaConnectConfig send config to kafka rest api
func (kcc *KCClient) PutKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName, body []byte) error {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s.%s:%d/connectors/%s/config", fmt.Sprintf("%s-service", kafkaConenctNamespacedname.Name), kafkaConenctNamespacedname.Namespace, port, connectorName)
	//url := fmt.Sprintf("http://%s.apps-crc.testing/connectors/%s/config", kafkaConenctNamespacedname.Name, connectorName)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		klog.Error(err, "error in the put")
	}

	defer res.Body.Close()

	if res.StatusCode > 399 {
		return fmt.Errorf("config update failed with error code %d", res.StatusCode)
	}
	bodyBytes, _ := ioutil.ReadAll(res.Body)
	klog.Infof("put res is %s", string(bodyBytes))
	return err

}

//GetAllConnectors get list connect names
func (kcc *KCClient) GetAllConnectors(kafkaConenctNamespacedname types.NamespacedName, port int32) ([]string, error) {
	url := fmt.Sprintf("http://%s.%s:%d/connectors", fmt.Sprintf("%s-service", kafkaConenctNamespacedname.Name), kafkaConenctNamespacedname.Namespace, port)
	//url := fmt.Sprintf("http://%s.apps-crc.testing/connectors", kafkaConenctNamespacedname.Name)
	response, err := httpClient.Get(url)
	if response.StatusCode > 399 {
		return make([]string, 0), fmt.Errorf("rest api return error, code : %d", response.StatusCode)
	}
	if err != nil {
		klog.Error(err, "error when getting current kafkaconnector connectors")
		return nil, err
	}
	data, _ := ioutil.ReadAll(response.Body)
	var output []string
	if err = json.Unmarshal(data, &output); err != nil {
		return nil, err
	}
	return output, nil

}
