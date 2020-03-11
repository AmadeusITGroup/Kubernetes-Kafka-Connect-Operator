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
	"testing"

	"github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/apis/kafkaconnect/v1alpha1"
	kafkaconnectv1alpha1 "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/apis/kafkaconnect/v1alpha1"
	"github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/custommetrics"
	k8sprovider "github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"

	listers "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/generated/listers/kafkaconnect/v1alpha1"
)

func TestReturnsCustomMetric(t *testing.T) {

	selector, _ := labels.Parse("")

	info := k8sprovider.CustomMetricInfo{
		Metric: "lag",
		GroupResource: schema.GroupResource{
			Resource: "Kafkaconnects",
		},
	}

	fakeKafkaMetricClient := fakeKafkaMetricsClient{
		lag: 15,
		err: nil,
	}

	s := scheme.Scheme
	s.AddKnownTypes(kafkaconnectv1alpha1.SchemeGroupVersion)

	provider := newFakeCustomProvider(fakeKafkaMetricClient)

	returnList, err := provider.GetMetricBySelector("Kafkaconnect", selector, info, selector)

	if err != nil {
		t.Errorf("error after processing got: %v, want nil", err)
	}

	if len(returnList.Items) != 1 {
		t.Errorf("returnList.Items length = %v, want there 1", len(returnList.Items))
	}

	customMetric := returnList.Items[0]
	if customMetric.Metric.Name != "lag" {
		t.Errorf("customMetric.Metric.Name = %v, want there %v", customMetric.Metric.Name, "lag")
	}

	if customMetric.Value.MilliValue() != int64(15000) {
		t.Errorf("customMetric.Value.MilliValue() = %v, want there %v", customMetric.Value.MilliValue(), int64(15000))
	}

}

func newFakeCustomProvider(f fakeKafkaMetricsClient) kafkaConnectProvider {

	provider := kafkaConnectProvider{
		kclister:           fakekafkaConnectLister{},
		kafkaMetricsClient: f,
	}
	return provider
}

type fakeKafkaMetricsClient struct {
	lag float64
	err error
}
type fakekafkaConnectLister struct {
}

var fackKafkaConnect *v1alpha1.KafkaConnect = v1alpha1.CreateFakeKafkaConnect()

func (f fakeKafkaMetricsClient) GetCustomMetric(request custommetrics.MetricRequest, metricSelector labels.Selector) (float64, error) {
	return f.lag, f.err
}

func (fakeLister fakekafkaConnectLister) List(selector labels.Selector) (ret []*v1alpha1.KafkaConnect, err error) {
	return []*v1alpha1.KafkaConnect{fackKafkaConnect}, nil
}

func (fakeLister fakekafkaConnectLister) KafkaConnects(namespace string) listers.KafkaConnectNamespaceLister {
	return fakeKafkaConnectNamespaceLister{}
}

type fakeKafkaConnectNamespaceLister struct {
}

func (fakeLister fakeKafkaConnectNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.KafkaConnect, err error) {
	return []*v1alpha1.KafkaConnect{fackKafkaConnect}, nil
}

func (fakeLister fakeKafkaConnectNamespaceLister) Get(name string) (*v1alpha1.KafkaConnect, error) {
	return fackKafkaConnect, nil
}
