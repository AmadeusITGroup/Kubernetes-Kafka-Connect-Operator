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

package provider

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/custommetrics"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/metrics/pkg/apis/custom_metrics"

	listers "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/generated/listers/kafkaconnect/v1alpha1"
)

type kafkaConnectProvider struct {
	kclister           listers.KafkaConnectLister
	kafkaMetricsClient custommetrics.KafkaMetricsClient
}

//NewKafkaConnectProvider create an new provider
func NewKafkaConnectProvider(kclister listers.KafkaConnectLister, kmclient custommetrics.KafkaMetricsClient) provider.CustomMetricsProvider {
	return &kafkaConnectProvider{

		kclister:           kclister,
		kafkaMetricsClient: kmclient,
	}
}

// GetMetricByName fetches a particular metric for a particular object.
// The namespace will be empty if the metric is root-scoped.
func (p *kafkaConnectProvider) GetMetricByName(name types.NamespacedName, info provider.CustomMetricInfo, metricSelector labels.Selector) (*custom_metrics.MetricValue, error) {

	klog.Infof("Received request for custom metric: groupresource: %s, namespace: %s, metric name: %s, object name: %s", info.GroupResource.String(), name.Namespace, info.Metric, name.Name)

	metricRequestInfo := custommetrics.NewMetricRequest(info, name)
	val, err := p.kafkaMetricsClient.GetCustomMetric(metricRequestInfo, metricSelector)
	if err != nil {
		return nil, err
	}
	kc, err := p.kclister.KafkaConnects(name.Namespace).Get(name.Name)
	if err != nil {
		return nil, err
	}
	ref := custom_metrics.ObjectReference{
		APIVersion: kc.APIVersion,
		Kind:       kc.Kind,
		Name:       name.Name,
		Namespace:  name.Namespace,
	}

	metricValue := &custom_metrics.MetricValue{
		DescribedObject: ref,
		Metric: custom_metrics.MetricIdentifier{
			Name: info.Metric,
		},
		Timestamp: metav1.Time{Time: time.Now()},
		Value:     *resource.NewQuantity(int64(val), resource.DecimalSI),
	}
	klog.Infof("return metric value %v", val)

	return metricValue, nil
}

func (p *kafkaConnectProvider) GetMetricBySelector(namespace string, selector labels.Selector, info provider.CustomMetricInfo, metricSelector labels.Selector) (*custom_metrics.MetricValueList, error) {
	klog.Infof("Received request for custom metric: groupresource: %s, namespace: %s, metric name: %s, selectors: %s", info.GroupResource.String(), namespace, info.Metric, selector.String())
	_, selectable := selector.Requirements()

	if !selectable {
		return nil, errors.NewBadRequest("label is set to not selectable. this should not happen")
	}

	kcs, err := p.kclister.List(selector)

	if err != nil {
		klog.Errorf("not able to list objects from api server: %v", err)
		return nil, errors.NewInternalError(fmt.Errorf("not able to list objects from api server for this resource"))
	}
	var wg sync.WaitGroup
	c := make(chan *custom_metrics.MetricValue, len(kcs))
	for _, kc := range kcs {

		wg.Add(1)
		namespacedName := types.NamespacedName{
			Namespace: kc.Namespace,
			Name:      kc.Name,
		}

		p.getMetricByName(&wg, c, namespacedName, info, metricSelector, selector)

	}
	wg.Wait()
	close(c)
	var metricList = make([]custom_metrics.MetricValue, len(c))
	var i int32
	for ele := range c {
		metricList[i] = *ele
		i++
	}
	return &custom_metrics.MetricValueList{
		Items: metricList,
	}, nil
}

// ListAllMetrics returns all custom metrics available from kafkaconnect .
// For the moment we list only lag metric
func (p *kafkaConnectProvider) ListAllMetrics() []provider.CustomMetricInfo {

	lists, err := p.kclister.List(labels.Everything())
	if err != nil {
		klog.Error("cannot get kafka connect", err)
		return nil
	}
	res := make([]provider.CustomMetricInfo, 0)

	gr := schema.GroupResource{Group: "kafkaconnect.operator.io", Resource: "kafkaconnect"}
	for _, kc := range lists {

		for _, config := range kc.Spec.KafkaConnectorsSpec.Configs {
			if config.ExposeLagMetric {
				res = append(res, provider.CustomMetricInfo{
					GroupResource: gr,
					Metric:        fmt.Sprintf("%s%s", config.Name, custommetrics.MetricSuffix),
					Namespaced:    true,
				})
			}
		}
	}

	return res
}

func (p *kafkaConnectProvider) getMetricByName(wg *sync.WaitGroup, c chan *custom_metrics.MetricValue, name types.NamespacedName, info provider.CustomMetricInfo, metricSelector labels.Selector, selector labels.Selector) {
	defer wg.Done()
	val, err := p.GetMetricByName(name, info, metricSelector)
	if err != nil {
		//TODO log error
		return
	}
	// add back the meta data about the request selectors
	if len(selector.String()) > 0 {
		labelSelector, err := metav1.ParseToLabelSelector(selector.String())
		if err != nil {
			//TODO log error
			return
		}
		val.Metric.Selector = labelSelector
	}
	c <- val

}
