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

package kafkaconnect

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	kafkaconnectv1alpha1 "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/apis/kafkaconnect/v1alpha1"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	extv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"k8s.io/client-go/tools/record"
)

// KCClient client to acess to kafka connect rest api
type fakeKCClient struct {
}

const configJSON string = "https://raw.githubusercontent.com/amadeusitgroup/kubernetes-kafka-connect-operator/master/connector-examples/connector1.json"
const wrongConfigJSON string = "https://raw.githubusercontent.com/amadeusitgroup/kubernetes-kafka-connect-operator/master/connector-examples/connector2.json"

var call int = 0

func (fkcc *fakeKCClient) GetKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (map[string]interface{}, error) {

	output := &map[string]interface{}{}
	if call > 0 {
		response, err := http.Get(configJSON)
		if err != nil {
			klog.Error(err, "error when getting current kafkaconnector config")
			return nil, err
		}
		data, _ := ioutil.ReadAll(response.Body)

		if err = json.Unmarshal(data, output); err != nil {
			return nil, err
		}
		(*output)["name"] = connectorName
	}
	call++
	return *output, nil
}

func (fkcc *fakeKCClient) PutKafkaConnectConfig(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName, body []byte) error {
	return nil
}

func (fkcc *fakeKCClient) GetKafkaConnectorRunningTaskNb(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) (int32, error) {
	return int32(0), nil
}

func (fkcc *fakeKCClient) GetAllConnectors(kafkaConenctNamespacedname types.NamespacedName, port int32) ([]string, error) {
	return []string{}, nil
}

func (fkcc *fakeKCClient) DeleteConnector(connectorName string, port int32, kafkaConenctNamespacedname types.NamespacedName) error {
	return nil
}

func TestKafkaConnectControllerDeploymentCreate(t *testing.T) {
	kafkaconnect := kafkaconnectv1alpha1.CreateFakeKafkaConnect()
	// Register the object in the fake client.
	objs := []runtime.Object{
		kafkaconnect,
	}

	// Register operator types with the runtime scheme.
	s := scheme.Scheme
	s.AddKnownTypes(kafkaconnectv1alpha1.SchemeGroupVersion, kafkaconnect)

	// Create a fake client to mock API calls.
	cl := fake.NewFakeClient(objs...)

	// Create a ReconcileKafkaConnect object with the scheme and fake client.
	r := &ReconcileKafkaConnect{client: cl, scheme: s, kcc: &fakeKCClient{}, eventRecorder: record.NewFakeRecorder(50)}

	// Mock request to simulate Reconcile() being called on an event for a
	// watched resource.
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      kafkaconnect.Name,
			Namespace: kafkaconnect.Namespace,
		},
	}
	res, err := r.Reconcile(req)
	if err != nil {
		t.Fatalf("1st reconcile: (%v)", err)
	}

	// Check the result of reconciliation to make sure it has the desired state.
	if res != (reconcile.Result{}) {
		t.Error("1st reconcile did not return an empty Result")
	}

	// Check the result of status initialization.
	kc := &kafkaconnectv1alpha1.KafkaConnect{}
	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	if *kc.Spec.KafkaConnectorsSpec.Configs[0].TasksMax != 2 {
		t.Fatalf("Connector TasksMax init failed: (expected: 2, found: %v)", *kc.Spec.KafkaConnectorsSpec.Configs[0].TasksMax)
	}

	expectedStatus := kafkaconnectv1alpha1.KafkaConnectStatus{
		KafkaConnectorStatus: make([]kafkaconnectv1alpha1.KafkaConnectorStatus, len(kafkaconnect.Spec.KafkaConnectorsSpec.Configs)),
		Updating:             true,
	}
	for i, config := range kafkaconnect.Spec.KafkaConnectorsSpec.Configs {
		expectedStatus.KafkaConnectorStatus[i] = kafkaconnectv1alpha1.KafkaConnectorStatus{
			Name:   config.Name,
			TaskNb: 0,
		}
	}

	foundStatus := kc.Status
	expectedStatus.LastScaleTime = foundStatus.LastScaleTime
	if !reflect.DeepEqual(expectedStatus, *foundStatus) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

	res, err = r.Reconcile(req)
	if err != nil {
		t.Fatalf("2nd reconcile: (%v)", err)
	}

	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	expectedStatus.PodNb = 2
	foundStatus = kc.Status
	if !reflect.DeepEqual(expectedStatus, *foundStatus) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

	// Check if deployment has been created and has the correct size.
	dep := &appsv1.Deployment{}
	err = r.client.Get(context.TODO(), req.NamespacedName, dep)
	if err != nil {
		t.Fatalf("get deployment: (%v)", err)
	}

	depJSONEncoded, err := json.MarshalIndent(dep, "", "  ")
	if err != nil {
		t.Fatalf("encode deployment: (%v)", err)
	}
	t.Log(string(depJSONEncoded))

	// Check if service has been created and has the correct size.
	svc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-service", kafkaconnect.Name), Namespace: kafkaconnect.Namespace}, svc)
	if err != nil {
		t.Fatalf("get service: (%v)", err)
	}

	svcJSONEncoded, err := json.MarshalIndent(svc, "", "  ")
	if err != nil {
		t.Fatalf("encode service: (%v)", err)
	}
	t.Log(string(svcJSONEncoded))

	// Check if ingress has been created and has the correct size.
	ing := &extv1beta1.Ingress{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-ingress", kafkaconnect.Name), Namespace: kafkaconnect.Namespace}, ing)
	if err != nil {
		t.Fatalf("get ingress: (%v)", err)
	}

	ingJSONEncoded, err := json.MarshalIndent(ing, "", "  ")
	if err != nil {
		t.Fatalf("encode ingress: (%v)", err)
	}
	t.Log(string(ingJSONEncoded))

	dep.Status.UpdatedReplicas = 2
	dep.Status.AvailableReplicas = 2
	dep.Status.Replicas = 2
	dep.Status.ObservedGeneration = dep.Generation

	r.client.Status().Update(context.TODO(), dep)
	// Mock a 3rd request to simulate Reconcile().
	res, err = r.Reconcile(req)
	if err != nil {
		t.Fatalf("3rd reconcile: (%v)", err)
	}

	// Check the result of reconciliation to make sure it has the desired state.
	if res != (reconcile.Result{}) {
		t.Error("3rd reconcile did not return an empty Result")
	}

	// Check the result of status initialization.
	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	expectedStatus.KafkaConnectorStatus[0].TaskNb = 2

	if !reflect.DeepEqual(expectedStatus, *kc.Status) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

	// Mock a 4th request to simulate Reconcile().
	res, err = r.Reconcile(req)
	if err != nil {
		t.Fatalf("4th reconcile: (%v)", err)
	}

	// Check the result of reconciliation to make sure it has the desired state.
	if res != (reconcile.Result{}) {
		t.Error("4th reconcile did not return an empty Result")
	}

	// Check the result of status initialization.
	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}
	if foundStatus.LastScaleTime.Time.After(time.Now()) {
		t.Fatalf("scale time not updated")
	}
	expectedStatus.LastScaleTime = foundStatus.LastScaleTime
	expectedStatus.Updating = false

	if !reflect.DeepEqual(expectedStatus, *kc.Status) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}
}

func TestKafkaConnectControllerWithWrongConnectorConfig(t *testing.T) {
	kafkaconnect := kafkaconnectv1alpha1.CreateFakeKafkaConnect()
	kafkaconnect.Spec.KafkaConnectorsSpec.Configs[0].URL = wrongConfigJSON
	// Register the object in the fake client.
	objs := []runtime.Object{
		kafkaconnect,
	}

	// Register operator types with the runtime scheme.
	s := scheme.Scheme
	s.AddKnownTypes(kafkaconnectv1alpha1.SchemeGroupVersion, kafkaconnect)

	// Create a fake client to mock API calls.
	cl := fake.NewFakeClient(objs...)

	// Create a ReconcileKafkaConnect object with the scheme and fake client.
	r := &ReconcileKafkaConnect{client: cl, scheme: s, kcc: &fakeKCClient{}, eventRecorder: record.NewFakeRecorder(50)}

	// Mock request to simulate Reconcile() being called on an event for a
	// watched resource.
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      kafkaconnect.Name,
			Namespace: kafkaconnect.Namespace,
		},
	}
	res, err := r.Reconcile(req)
	if err != nil {
		t.Fatalf("1st reconcile: (%v)", err)
	}

	// Check the result of reconciliation to make sure it has the desired state.
	if res != (reconcile.Result{}) {
		t.Error("1st reconcile did not return an empty Result")
	}

	// Check the result of status initialization.
	kc := &kafkaconnectv1alpha1.KafkaConnect{}
	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	if *kc.Spec.KafkaConnectorsSpec.Configs[0].TasksMax != 0 {
		t.Fatalf("Connector TasksMax init failed: (expected: 0, found: %v)", *kc.Spec.KafkaConnectorsSpec.Configs[0].TasksMax)
	}

	expectedStatus := kafkaconnectv1alpha1.KafkaConnectStatus{
		KafkaConnectorStatus: make([]kafkaconnectv1alpha1.KafkaConnectorStatus, len(kafkaconnect.Spec.KafkaConnectorsSpec.Configs)),
		Updating:             true,
	}
	for i, config := range kafkaconnect.Spec.KafkaConnectorsSpec.Configs {
		expectedStatus.KafkaConnectorStatus[i] = kafkaconnectv1alpha1.KafkaConnectorStatus{
			Name:   config.Name,
			TaskNb: 0,
			Error:  fmt.Sprintf("cannot get int value of tasks.max from config for %s", config.Name),
		}
	}

	foundStatus := kc.Status
	expectedStatus.LastScaleTime = kc.Status.LastScaleTime
	if !reflect.DeepEqual(expectedStatus, *foundStatus) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

	res, err = r.Reconcile(req)
	if err != nil {
		t.Fatalf("2nd reconcile: (%v)", err)
	}

	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	foundStatus = kc.Status
	if !reflect.DeepEqual(expectedStatus, *foundStatus) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

	// Check if deployment has been created and has the correct size.
	dep := &appsv1.Deployment{}
	err = r.client.Get(context.TODO(), req.NamespacedName, dep)
	if err != nil {
		t.Fatalf("get deployment: (%v)", err)
	}

	depJSONEncoded, err := json.MarshalIndent(dep, "", "  ")
	if err != nil {
		t.Fatalf("encode deployment: (%v)", err)
	}
	t.Log(string(depJSONEncoded))

	// Check if service has been created and has the correct size.
	svc := &corev1.Service{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-service", kafkaconnect.Name), Namespace: kafkaconnect.Namespace}, svc)
	if err != nil {
		t.Fatalf("get service: (%v)", err)
	}

	svcJSONEncoded, err := json.MarshalIndent(svc, "", "  ")
	if err != nil {
		t.Fatalf("encode service: (%v)", err)
	}
	t.Log(string(svcJSONEncoded))

	// Check if ingress has been created and has the correct size.
	ing := &extv1beta1.Ingress{}
	err = r.client.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("%s-ingress", kafkaconnect.Name), Namespace: kafkaconnect.Namespace}, ing)
	if err != nil {
		t.Fatalf("get ingress: (%v)", err)
	}

	ingJSONEncoded, err := json.MarshalIndent(ing, "", "  ")
	if err != nil {
		t.Fatalf("encode ingress: (%v)", err)
	}
	t.Log(string(ingJSONEncoded))

	dep.Status.UpdatedReplicas = 0
	dep.Status.AvailableReplicas = 0
	dep.Status.Replicas = 0
	dep.Status.ObservedGeneration = dep.Generation

	r.client.Status().Update(context.TODO(), dep)
	// Mock a 3rd request to simulate Reconcile().
	res, err = r.Reconcile(req)
	if err != nil {
		t.Fatalf("3rd reconcile: (%v)", err)
	}

	// Check the result of reconciliation to make sure it has the desired state.
	if res != (reconcile.Result{}) {
		t.Error("3rd reconcile did not return an empty Result")
	}

	// Check the result of status initialization.
	err = r.client.Get(context.TODO(), req.NamespacedName, kc)
	if err != nil {
		t.Fatalf("get KafkaConnect: (%v)", err)
	}

	if foundStatus.LastScaleTime.Time.After(time.Now()) {
		t.Fatalf("scale time not updated")
	}
	expectedStatus.LastScaleTime = foundStatus.LastScaleTime
	expectedStatus.Updating = false
	if !reflect.DeepEqual(expectedStatus, *kc.Status) {
		t.Fatalf("check status initialization failed: (expected: %v, found: %v)", expectedStatus, *foundStatus)
	}

}
