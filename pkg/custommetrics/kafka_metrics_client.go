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

package custommetrics

import (
	"context"
	"fmt"
	"strings"
	"sync"

	kafka "github.com/Shopify/sarama"
	kafkaconnectv1alpha1 "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/apis/kafkaconnect/v1alpha1"
	kcc "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/kafkaconnectclient"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	apiVersion     = "v1"
	kafkaServerVer = "2.0.1"
	//MetricSuffix the suffix of lag metric
	MetricSuffix = "-lag"
)

// KafkaMetricsClient provides methods for accessing Kafka clusters APIs
type KafkaMetricsClient interface {
	GetCustomMetric(request MetricRequest, metricSelector labels.Selector) (float64, error)
}

// KafkaTopicMetricsClient is used to call kafka to get metrics per client
type KafkaTopicMetricsClient struct {
	client    client.Client
	corev1Itf corev1.CoreV1Interface
}

// NewClient creates a client for calling Application
func NewClient(client client.Client, corev1Itf corev1.CoreV1Interface) KafkaMetricsClient {
	return KafkaTopicMetricsClient{
		client:    client,
		corev1Itf: corev1Itf,
	}
}

// GetCustomMetric calls kafka to retrieve the value of the metric requested
func (c KafkaTopicMetricsClient) GetCustomMetric(request MetricRequest, metricSelector labels.Selector) (float64, error) {
	metricsResult, err := c.getMetric(request)
	if err != nil {
		return 0, err
	}
	return *metricsResult, err
}

// GetMetric calls to API to retrieve a specific metric
func (c KafkaTopicMetricsClient) getMetric(metricInfo MetricRequest) (*float64, error) {

	kc := &kafkaconnectv1alpha1.KafkaConnect{}
	c.client.Get(context.TODO(), metricInfo.namespacedname, kc)

	brokerString := kc.Spec.KafkaConnectorsSpec.KafkaBrokers
	brokers := strings.Split(brokerString, ",")
	configName := strings.TrimSuffix(metricInfo.info.Metric, MetricSuffix)
	//get request config
	var foundConfig *kafkaconnectv1alpha1.KafkaConnectorConfig
	for i := range kc.Spec.KafkaConnectorsSpec.Configs {
		if kc.Spec.KafkaConnectorsSpec.Configs[i].Name == configName {
			foundConfig = &kc.Spec.KafkaConnectorsSpec.Configs[i]
			break
		}
	}
	if foundConfig == nil {
		return nil, errors.NewBadRequest("cannot find config for metric")
	}
	conf, err := kcc.GetKafkaConnectConfig(
		c.corev1Itf.ConfigMaps(metricInfo.namespacedname.Namespace),
		*foundConfig)
	if err != nil {
		return nil, err
	}

	topic := conf["topics"].(string)

	partitions, err := getPartitions(brokers, topic)
	if err != nil {
		return nil, err
	}

	group := fmt.Sprintf("connect-%s", configName)

	offsetTotal, err := fetchGroupTotalLag(brokers, group, topic, partitions)
	if err != nil {
		return nil, err
	}
	res := float64(offsetTotal)
	return &res, err

}

// MetricRequest represents options
type MetricRequest struct {
	info           provider.CustomMetricInfo
	namespacedname types.NamespacedName
}

// NewMetricRequest creates a new metric request with defaults for optional parameters
func NewMetricRequest(info provider.CustomMetricInfo, namespacedname types.NamespacedName) MetricRequest {
	return MetricRequest{
		info:           info,
		namespacedname: namespacedname,
	}
}

type partitionOffset struct {
	Partition int32
	Lag       int64
}

func fetchLag(wg *sync.WaitGroup, client *kafka.Client, offsetManager *kafka.OffsetManager, topic string, partition int32, c chan partitionOffset) {
	defer wg.Done()
	pom, err := (*offsetManager).ManagePartition(topic, partition)
	defer func() {
		if pom != nil {
			pom.Close()
		}
	}()

	if err != nil {
		klog.Error(err, "cannot get partiton manager", "topic", topic, "partition", partition)

	} else {
		offset, _ := pom.NextOffset()
		if newestOffset, err := (*client).GetOffset(topic, partition, kafka.OffsetNewest); err != nil {
			klog.Error(err, "cannot get newest offset")
		} else {

			c <- partitionOffset{
				Partition: partition,
				Lag:       newestOffset - offset,
			}
		}
	}
}

func fetchGroupTotalLag(brokers []string, group, topic string, partitions []int32) (int64, error) {
	version, err := kafka.ParseKafkaVersion(kafkaServerVer)
	if err != nil {
		return 0, err
	}
	config := kafka.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = kafka.OffsetOldest
	client, err := kafka.NewClient(brokers, config)

	defer func() {
		if client != nil {
			client.Close()
		}
	}()

	if err != nil {
		return 0, err
	}

	offsetManager, err := kafka.NewOffsetManagerFromClient(group, client)
	defer func() {
		if offsetManager != nil {
			offsetManager.Close()
		}
	}()

	if err != nil {
		return 0, err
	}
	partitionLen := len(partitions)

	var offsetTotal int64

	var wg sync.WaitGroup

	c := make(chan partitionOffset, partitionLen)
	for _, partition := range partitions {
		wg.Add(1)
		go fetchLag(&wg, &client, &offsetManager, topic, partition, c)
	}
	wg.Wait()
	close(c)
	for val := range c {
		offsetTotal += val.Lag
	}
	return offsetTotal, nil

}

func getPartitions(brokers []string, topic string) (res []int32, err error) {

	version, err := kafka.ParseKafkaVersion(kafkaServerVer)
	if err != nil {
		return
	}
	config := kafka.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true

	client, err := kafka.NewClient(brokers, config)

	defer func() {
		if client != nil {
			client.Close()
		}
	}()
	if err != nil {
		return
	}
	partitions, err := client.Partitions(topic)
	if err != nil {
		return

	}
	return partitions, nil

}
