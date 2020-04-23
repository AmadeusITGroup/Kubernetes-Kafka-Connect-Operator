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

package v1alpha1

import (
	autoscalingv2beta2 "k8s.io/api/autoscaling/v2beta2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/**********************************
*
*start of the kafka connect api
*
***********************************/
// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.
// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
// Important: Run "operator-sdk generate crds" to regenerate code after modifying this file

// KafkaConnectSpec defines the desired state of KafkaConnect
// this resource will create a Deployment and a Service by default
// if IngressSpec is given, then it will also create an ingress obj
type KafkaConnectSpec struct {
	// PodSpec use the default k8s.io/api/core/v1/PodSpec
	// for more information check https://godoc.org/k8s.io/api/core/v1#PodSpec
	PodSpec corev1.PodSpec `json:"podSpec"`
	// IngressSpec define how the ingress object should be created
	// special role should be given to the related service account.
	// if nil no ingress object will be created by operator, you can always creat your own ingress
	IngressSpec *IngressSpec `json:"ingressSpec,omitempty"`
	// KafkaConnectorsSpec define the different
	KafkaConnectorsSpec *KafkaConnectorsSpec `json:"connectors,omitempty"`
	//scaleStabilizationSec is the duration in sec between each scale action
	ScaleStabilizationSec *int64 `json:"scaleStabilizationSec,omitempty"`
	// RestApiPort tell which one is the port for rest api, if more than one port exist in pod spec
	KafkaConnectRestAPIPort int32 `json:"kafkaConnectRestAPIPort"`
}

type ConfigMap struct {
	Name string `json:"name"`
	Item string `json:"item"`
}

// IngressSpec defines how we need to create ingress to expose the kafka connect rest api
type IngressSpec struct {
	// Style define the style of the ingress created, either a subDomain or a path
	Style *IngressStyle `json:"style,omitempty"`
	// the parent domain from where the ingress will be created
	ParentDomain string `json:"parentDomain"`
}

// IngressStyle define the style of the ingress created, either a subDomain or a path
type IngressStyle string

const (
	// PathStyle will create ingress like parentDomain/kafkaconnectName
	PathStyle IngressStyle = "pathStyle"
	// DomainStyle will create ingress like kafkaconnectName.parentDomain
	DomainStyle IngressStyle = "domainStyle"
)

// KafkaConnectorsSpec define the list of the connector config and the number of task per pod
type KafkaConnectorsSpec struct {
	// connectorConfigs is the list of the kafka connector config
	Configs []KafkaConnectorConfig `json:"connectorConfigs,omitempty"`
	// taskPerPod is the number of the task per pod, default: 1, if <=0 autoscaler will never work and initPodReplicas will be used to create deployment
	TaskPerPod *int32 `json:"taskPerPod,omitempty"`
	//initPodReplicas is the initial number of the pod if TaskPerPod is <=0
	InitDeploymentReplicas *int32 `json:"initPodReplicas,omitempty"`
	//keepUnknownConnectors by default false, if false, it will delete all unknown connector in the cluster
	KeepUnknownConnectors bool `json:"keepUnknownConnectors,omitempty"`
	//kafkaBrokers is the brokers of kafka cluster
	KafkaBrokers string `json:"kafkaBrokers"`
}

// KafkaConnectorConfig define one connector will be created if it's different from the existing one
type KafkaConnectorConfig struct {
	// Name is the Name should be used as the connector
	Name string `json:"name"`
	// URL is the link will connector config could be found
	URL *string `json:"url,omitempty"`
	// configMap is the link will connector config could be found
	ConfigMap *ConfigMap `json:"configMap,omitempty"`
	// TasksMax define number of task for connector it will override the value in config. the default value is the nb defined in the config
	TasksMax *int32 `json:"taskMax,omitempty"`
	//exposeLagMetric tell if lag should be expose or not
	ExposeLagMetric bool `json:"exposeLagMetric,omitempty"`
}

// KafkaConnectorStatus defines the observed state of each KafkaConnector
// +k8s:openapi-gen=true
type KafkaConnectorStatus struct {
	Name   string `json:"name"`
	TaskNb int32  `json:"taskNb"`
	Error  string `json:"error,omitempty"`
}

// KafkaConnectStatus defines the observed state of KafkaConnect
// +k8s:openapi-gen=true
type KafkaConnectStatus struct {
	KafkaConnectorStatus []KafkaConnectorStatus `json:"connectorStatus"`
	PodNb                int32                  `json:"podNb"`
	Updating             bool                   `json:"updating"`
	LastScaleTime        metav1.Time            `json:"lastScaleTime"`
}

// KafkaConnect is the Schema for the kafkaconnects API
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=kafkaconnects,scope=Namespaced
// +genclient
type KafkaConnect struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaConnectSpec    `json:"spec"`
	Status *KafkaConnectStatus `json:"status,omitempty"`
}

// KafkaConnectList contains a list of KafkaConnect
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KafkaConnectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaConnect `json:"items"`
}

/**********************************
*
*end of the kafka connect api
*
***********************************/

/**********************************
*
*start of the kafka connect scaler api
*
***********************************/
// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// KafkaConnectAutoScalerSpec defines the desired state of KafkaConnectAutoScaler
// +k8s:openapi-gen=true
type KafkaConnectAutoScalerSpec struct {
	// kcScaleTargetRef points to the kafka connector to scale, and is used to the kafkaconnect for which metrics
	// should be collected, as well as to actually change the replica count.
	KafkaConnectorScaleTargetRef KafkaConnectorReference `json:"kcScaleTargetRef" protobuf:"bytes,1,opt,name=kcScaleTargetRef"`
	// minTasks is the lower limit for the number of replicas to which the autoscaler
	// can scale down.  It defaults to 1 pod.  minTasks is allowed to be 0 if the
	// alpha feature gate HPAScaleToZero is enabled and at least one Object or External
	// metric is configured.  Scaling is active as long as at least one metric value is
	// available.
	// +optional
	MinTasks *int32 `json:"minTasks,omitempty" protobuf:"varint,2,opt,name=minTasks"`
	// maxTasks is the upper limit for the number of replicas to which the autoscaler can scale up.
	// It cannot be less that minTasks.
	MaxTasks int32 `json:"maxTasks" protobuf:"varint,3,opt,name=maxTasks"`
	// metrics contains the specifications for which to use to calculate the
	// desired replica count (the maximum replica count across all metrics will
	// be used).  The desired replica count is calculated multiplying the
	// ratio between the target value and the current value by the current
	// number of pods.  Ergo, metrics used must decrease as the pod count is
	// increased, and vice-versa.  See the individual metric source types for
	// more information about how each type of metric must respond.
	// If not set, the default metric will be set to 80% average CPU utilization.
	// +optional
	Metrics []autoscalingv2beta2.MetricSpec `json:"metrics,omitempty" protobuf:"bytes,4,rep,name=metrics"`
}

// KafkaConnectorReference contains enough information to let you identify the referred kafka connector.
type KafkaConnectorReference struct {
	// the name of the kafkaconnect CR
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`
	// API version of the referent
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,3,opt,name=apiVersion"`
	//kafkaConnectorName name of a specific kafka connector in the kafka connect resource object
	KafkaConnectorName string `json:"kafkaConnectorName"`
}

// KafkaConnectAutoScalerStatus defines the observed state of KafkaConnectAutoScaler
// +k8s:openapi-gen=true
type KafkaConnectAutoScalerStatus struct {
	autoscalingv2beta2.HorizontalPodAutoscalerStatus `json:",inline"`
}

// KafkaConnectAutoScaler is the Schema for the kafkaconnectautoscalers API
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=kafkaconnectautoscalers,scope=Namespaced
// +genclient
type KafkaConnectAutoScaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaConnectAutoScalerSpec   `json:"spec,omitempty"`
	Status KafkaConnectAutoScalerStatus `json:"status,omitempty"`
}

// KafkaConnectAutoScalerList contains a list of KafkaConnectAutoScaler
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type KafkaConnectAutoScalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaConnectAutoScaler `json:"items"`
}

/**********************************
*
*end of the kafka connect scaler api
*
***********************************/

func init() {
	SchemeBuilder.Register(&KafkaConnect{}, &KafkaConnectList{})
	SchemeBuilder.Register(&KafkaConnectAutoScaler{}, &KafkaConnectAutoScalerList{})
}

//CreateFakeKafkaConnect is the function to create kafkaconnect object for unit test only
func CreateFakeKafkaConnect() *KafkaConnect {
	var (
		name      = "connector-elastic"
		namespace = "kafkaconnect"
		labels    = map[string]string{
			"app":  "kafkaconnect",
			"type": "elasticsearch-sink",
		}
		port            int32               = 8083
		style           IngressStyle        = "domainStyle"
		parentDomain                        = "apps-crc.testing"
		taskPerPod      int32               = 1
		connectorName                       = "connector-elastic"
		connectorURL                        = "https://raw.githubusercontent.com/amadeusitgroup/kubernetes-kafka-connect-operator/master/connector-examples/connector1.json"
		image                               = "test/kafkaconnectdockerimage:latest"
		imagePullPolicy corev1.PullPolicy   = "IfNotPresent"
		resourceLimits  corev1.ResourceList = map[corev1.ResourceName]resource.Quantity{
			"cpu": {
				Format: "200m",
			},
			"memory": {
				Format: "300Mi",
			},
		}
		resourceRequests corev1.ResourceList = map[corev1.ResourceName]resource.Quantity{
			"cpu": {
				Format: "100m",
			},
			"memory": {
				Format: "80Mi",
			},
		}
	)

	// Create a KafkaConnect object.
	kafkaconnect := &KafkaConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: KafkaConnectSpec{
			KafkaConnectRestAPIPort: port,
			IngressSpec: &IngressSpec{
				Style:        &style,
				ParentDomain: parentDomain,
			},
			KafkaConnectorsSpec: &KafkaConnectorsSpec{
				Configs: []KafkaConnectorConfig{
					{
						Name: connectorName,
						URL:  &connectorURL,
					},
				},
				TaskPerPod: &taskPerPod,
			},
			PodSpec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:            namespace,
						Image:           image,
						ImagePullPolicy: imagePullPolicy,
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: port,
							},
						},
						Resources: corev1.ResourceRequirements{
							Limits:   resourceLimits,
							Requests: resourceRequests,
						},
					},
				},
			},
		},
	}
	return kafkaconnect
}
