# API Specification
Packages : 
 - [kafkaconnect.operator.io/v1alpha1](#v1alpha1)
## kafkaconnect.operator.io/v1alpha1<a name="v1alpha1"></a>

Resource Types:
 - [KafkaConnect](#v1alpha1_kafkaconnect)
 - [KafkaConnectAutoScaler](#v1alpha1_kafkaconnectautoscaler)

### KafkaConnect<a name="v1alpha1_kafkaconnect"></a>

 Field | Description
 --- | ---
`apiVersion`<br> string | kafkaconnect.operator.io/v1alpha1
`kind` <br> string | KafkaConnect
`metadata` <br> [Kubernetes meta/v1.ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#objectmeta-v1-meta) | Refer to the Kubernetes API documentation for the fields of the `metadata` field.
`spec` <br> [KafkaConnectSpec](#v1alpha1_kafkaconnect_spec) | Refer to [KafkaConnectSpec](#v1alpha1_kafkaconnect_spec) below
`status` <br> [KafkaConnectStatus](#v1alpha1_kafkaconnect_status) | *(optional)* <br> Refer to [KafkaConnectStatus](#v1alpha1_kafkaconnect_status) below

### KafkaConnectSpec<a name="v1alpha1_kafkaconnect_spec"></a>

 Field | Description
 --- | ---
 `connectors` <br> [KafkaConnectSpecConnectors](#v1alpha1_kafkaconnect_spec_connectors) | *(optional)* <br> this field define the different connector run in this kafka connect cluster and ratio between connecto task and pod number for the autoscaler
 `ingressSpec` <br> [KafkaConnectSpecIngressSpec](#v1alpha1_kafkaconnect_spec_ingressspec) | *(optional)* <br> this spec define how to create an ingress object to expose kafkaconnect rest api, check below section for more detail. The ingress object will not be created if this spec is absent.
 `kafkaConnectRestAPIPort` <br> int32 | the kafka connect rest api port, operator will create service on this port and connect to this port to read/update connectors' config and status
`podSpec`<br> [Kubernetes core/v1.PodSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#podspec-v1-core) | Refer to the Kubernetes API documentation for the fields of the `podSpec` field. The Kafka Connect Operator will create [kubernetes deployment](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#deployment-v1-apps) object base on the podSpec here
`scaleStabilizationSec`<br> int64 | *(optional)* <br> the duration in sec between each scale action, default 300

### KafkaConnectSpecConnectors<a name="v1alpha1_kafkaconnect_spec_connectors"></a>
 Field | Description
 --- | ---
`connectorConfigs`<br> [KafkaConnectSpecConnectorsConnectorConfigs](#v1alpha1_kafkaconnect_spec_connectors_connectorconfigs) array | this field define each connector in this cluster, their task max and whether we should expose the lag as a metric or not.
`initPodReplicas`<br> int32 | *(optional)* <br> Default: 1 <br> the initial Kubernetes Deployement replicas number. If `taskPerPod` > 0, the replicas number will be auto-scaled base on the total number of kafka connector tasks.
`kafkaBrokers`<br> string | Kafka brokers, seprated by `,`
`keepUnknownConnectors`<br> bool | *(optional)* <br> Default: false <br> If false, it will delete all connector not defined in `connectorConfigs` during each reconcil action
`taskPerPod`<br> int32 | *(optional)* <br> Default: 1 <br> The number of the kafka connector task per pod. This number will be used to auto-scale deployment's replicas base on the total number of kafka connector tasks in this cluster. If `taskPerPod`<=0, deployment's replicas will not be auto-scaled and `initPodReplicas` will be used in kubernetes deployment replicas

### KafkaConnectSpecConnectorsConnectorConfigs<a name="v1alpha1_kafkaconnect_spec_connectors_connectorconfigs"></a>
 Field | Description
 --- | ---
 `name` <br> string | the name of the connector
 `url` <br> string | the link of a [connector config](https://kafka.apache.org/documentation/#connect_configuring)
 `taskMax` <br> int32 | *(optional)* <br> the `tasks.max` in the connector config. If not defined, during the init step, operator will fill this field by the value in the connector config. The value in this field will overwrite the value in the connector config and it will be updated by auto-scaler
 `exposeLagMetric` <br> boolean | *(optional)* <br> default: false <br> If the operator need to expose the connector's total lag in a customer metric or not. If true a metric with name `${connectorName}-lag` will be exposed for more example, check [the autoscaler example](../examples/v1alpha1/kafkaconnectautoscaler.yaml)

### KafkaConnectSpecIngressSpec<a name="v1alpha1_kafkaconnect_spec_ingressspec"></a>
 Field | Description
 --- | ---
 `parentDomain` <br> string | the parent domain from what the ingress will be created
 `style` <br> enum <br> [pathStyle, domainStyle] | *(optional)* <br> default: pathStyle <br> this field define how the ingress will be created. If `style` = `pathStyle`, ingress will be created as *\${parentDomain}*/*\${metadata.name}* Else if `style` = `domainStyle`, ingress will be created as *\${metadata.name}*.*\${parentDomain}*


### KafkaConnectStatus<a name="v1alpha1_kafkaconnect_status"></a>
 Field | Description
 --- | ---
 `connectorStatus` <br> [KafkaConnectStatusConnectorStatus](#v1alpha1_kafkaconnect_status_connectorstatus) array | status of each connector in `connectorConfigs` field
 `lastScaleTime` <br> date-time | the date-time of last auto-scale operation
 `podNb` <br> int32 | the current replicas number of the generated kubernetes deployment
 `updating` <br> bool | if this kafkaconnect object is being updated, due to object creation or autoscale
 
### KafkaConnectStatusConnectorStatus<a name="v1alpha1_kafkaconnect_status_connectorstatus"></a>
 Field | Description
 --- | ---
 `name` <br> string | The name defined `connectorConfigs`
 `error` <br> string | The error message related to the connector config read or update via rest api
 `taskNb` <br> int32 | The current task number of this kafka connector

### KafkaConnectAutoScaler<a name="v1alpha1_kafkaconnectautoscaler"></a>
 Field | Description
 --- | ---
 `apiVersion`<br> string | kafkaconnect.operator.io/v1alpha1
 `kind` <br> string | KafkaConnectAutoScaler
 `metadata` <br> [Kubernetes meta/v1.ObjectMeta](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#objectmeta-v1-meta) | Refer to the Kubernetes API documentation for the fields of the `metadata` field.
 `spec` <br> [KafkaConnectAutoScalerSpec](#v1alpha1_kafkaconnectautoscaler_spec) | Refer to [KafkaConnectAutoScalerSpec](#v1alpha1_kafkaconnectautoscaler_spec) below
 `status` <br> [Kubernetes autoscaling/v2beta2 HorizontalPodAutoscalerStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#horizontalpodautoscalerstatus-v2beta2-autoscaling) | *(optional)* <br> Refer to [Kubernetes autoscaling/v2beta2 HorizontalPodAutoscalerStatus](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#horizontalpodautoscalerstatus-v2beta2-autoscaling)

### KafkaConnectAutoScalerSpec<a name="v1alpha1_kafkaconnectautoscaler_spec"></a>
 Field | Description
 --- | ---
 kcScaleTargetRef <br> [KafkaConnectorReference](#v1alpha1_kafkaconnectautoscaler_spec_kafkaconnectorreference) | The field refer to a kafka connector in a specific kafka connect object
 minTasks <br> int32 | *(optional)* <br> default: 1 <br> this is the lower limit for the number of tasks to which the autoscaler can scale down.
 maxTasks <br> int32 | this is the upper limit for the number of tasks to which the autoscaler can scale up.
 metrics <br> [Kubernetes autoscaling/v2beta2 MetricSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#metricspec-v2beta2-autoscaling) array | Refer to [Kubernetes autoscaling/v2beta2 MetricSpec](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.13/#metricspec-v2beta2-autoscaling)

 ### KafkaConnectorReference<a name="v1alpha1_kafkaconnectautoscaler_spec_kafkaconnectorreference"></a>
 Field | Description
 --- | ---
 `name` <br> string | kafka connect object name
 `apiVersion` <br> string | kafkaconnect.operator.io/v1alpha1
 `kafkaConnectorName` <br> string | kafka connector name