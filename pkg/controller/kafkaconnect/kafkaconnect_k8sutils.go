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
	e "errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	kafkaconnectv1alpha1 "github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/apis/kafkaconnect/v1alpha1"
	"github.com/amadeusitgroup/kubernetes-kafka-connect-operator/pkg/kafkaconnectclient"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	encodedDeploymentAnnotation = "kafkaconnect/encoded-deployment"
	encodedServiceAnnotation    = "kafkaconnect/encoded-service"
	encodedIngressAnnotation    = "kafkaconnect/encoded-ingress"
)

// Utils reconciles a KafkaConnect object
type Utils struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	ReconcileKafkaConnect
	specUpdated bool
}

// initConnectorTaskMax init a taskmax for each connector
func (utils *Utils) initConnectorTaskMax(wg *sync.WaitGroup, spec *kafkaconnectv1alpha1.KafkaConnectorConfig, connectorStatus *kafkaconnectv1alpha1.KafkaConnectorStatus, instance *kafkaconnectv1alpha1.KafkaConnect) {
	defer wg.Done()
	if spec.TasksMax != nil {
		return
	}
	nb := int32(0)
	utils.specUpdated = true
	//get config bytes from the url
	config, err := kafkaconnectclient.GetKafkaConnectConfig(utils.corev1Itf.ConfigMaps(instance.Namespace), *spec)
	// if there is error
	if err != nil {
		spec.TasksMax = &nb
		utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "UnableToGetConfig", "cannot get config for connector %s", spec.Name)
		connectorStatus.Error = fmt.Sprintf("cannot get config for %s", spec.Name)
		klog.Error(err, "cannot get config")
		return
	}

	//read tasks.max
	taskNb := config["tasks.max"]
	// if string
	if nbStr, ok := taskNb.(string); ok {
		nbInt := 0
		if nbInt, err = strconv.Atoi(nbStr); err != nil {
			spec.TasksMax = &nb
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "UnableToGetTasksMax", "cannot get tasks.max from config for connector %s", spec.Name)
			connectorStatus.Error = fmt.Sprintf("cannot get int value of tasks.max from config for %s", spec.Name)
			klog.Error(err, "cannot get int value of tasks.max from config")
			return
		}
		nb = int32(nbInt)
	} else if nbInt, ok := taskNb.(int); ok {
		nb = int32(nbInt)
	}

	if nb > 0 {
		spec.TasksMax = &nb
	}
}

func (utils *Utils) getTotalTaskNb(instance *kafkaconnectv1alpha1.KafkaConnect) int32 {
	res := int32(0)
	for _, connectorSpec := range instance.Spec.KafkaConnectorsSpec.Configs {
		res = res + *connectorSpec.TasksMax
	}
	return res
}

func (utils *Utils) checkUpdatingStatus(instance *kafkaconnectv1alpha1.KafkaConnect, changed bool) {
	if changed && !instance.Status.Updating {
		instance.Status.Updating = true
	}
	if !changed && instance.Status.Updating {
		instance.Status.Updating = false
		instance.Status.LastScaleTime = metav1.Time{Time: time.Now()}
	}
}

func (utils *Utils) updateDefaultTaskMax(instance *kafkaconnectv1alpha1.KafkaConnect) {
	var wg sync.WaitGroup
	for i := range instance.Spec.KafkaConnectorsSpec.Configs {
		wg.Add(1)
		go utils.initConnectorTaskMax(&wg, &instance.Spec.KafkaConnectorsSpec.Configs[i], &instance.Status.KafkaConnectorStatus[i], instance)
	}
	wg.Wait()
}

// CheckGlobalStatus check all the status add see which object need to be updated
func (utils *Utils) CheckGlobalStatus(instance *kafkaconnectv1alpha1.KafkaConnect) error {

	oriInstance := instance.DeepCopy()
	// if no status, create new status from config and update
	if instance.Status == nil {
		instance.Status = &kafkaconnectv1alpha1.KafkaConnectStatus{
			Updating:             true,
			KafkaConnectorStatus: make([]kafkaconnectv1alpha1.KafkaConnectorStatus, len(instance.Spec.KafkaConnectorsSpec.Configs)),
			LastScaleTime:        metav1.Time{Time: time.Now()},
			PodNb:                0,
		}
		for i, configSpec := range instance.Spec.KafkaConnectorsSpec.Configs {
			instance.Status.KafkaConnectorStatus[i] = kafkaconnectv1alpha1.KafkaConnectorStatus{
				Name:   configSpec.Name,
				TaskNb: int32(0),
				Error:  "",
			}
		}
	}
	utils.updateDefaultTaskMax(instance)
	if utils.specUpdated {
		klog.Info("task max updated")
		err := utils.client.Update(context.TODO(), instance)
		if err != nil {
			klog.Errorf("cannot update the kafka connect object %s:%s, %s",
				instance.Namespace, instance.Name, err.Error())
		}
		return err
	}

	// if status exist, check deployement config, service and ingress object
	depIsUpdating := false
	changed := false
	var foundDep *appsv1.Deployment
	var foundSvc *corev1.Service
	if dep, foundDeployment, err := utils.checkDeployment(instance); err != nil {
		return err
	} else if dep != nil {
		changed = true
		depIsUpdating = true
		klog.Infof("udpate dep %s/%s", dep.Namespace, dep.Name)
		if err = controllerutil.SetControllerReference(instance, dep, utils.scheme); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedSetOwnerReference", "failed to set owner reference for deployment %s", err.Error())
			return err
		}
		if dep.Annotations == nil {
			dep.Annotations = make(map[string]string)
		}
		exist := foundDeployment != nil
		if err = utils.apply(dep, exist); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedApplyDeployment", "failed to apply deployment %s", err.Error())
			return err
		}
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "SuccessedApplyDeployment", "successed create deployment %s/%s", dep.Namespace, dep.Name)

	} else if instance.Status.PodNb > *foundDeployment.Spec.Replicas {
		depIsUpdating = true
		changed = true
		err = utils.ScaleDeployment(instance, foundDeployment)
		if err != nil {
			utilruntime.HandleError(err)
			return err
		}
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "ScaleUpDeployment", "deployment exist already need to scale up %s/%s", foundDeployment.Namespace, foundDeployment.Name)
		foundDep = foundDeployment
	} else {
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "DeploymentExist", "deployment exist already %s/%s", foundDeployment.Namespace, foundDeployment.Name)
		foundDep = foundDeployment
	}

	if svc, foundService, err := utils.checkService(instance); err != nil {
		return err
	} else if svc != nil {
		changed = true
		klog.Infof("udpate svc %s/%s", svc.Namespace, svc.Name)
		if err = controllerutil.SetControllerReference(instance, svc, utils.scheme); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedSetOwnerReference", "failed to set owner reference for service %s", err.Error())
			return err
		}
		exist := (foundService != nil)
		if err = utils.apply(svc, exist); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedApplyService", "failed to apply service %s", err.Error())
			return err
		}
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "SuccessedApplyService", "successed create service %s/%s", svc.Namespace, svc.Name)
	} else {
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "ServiceExist", "service exist already %s/%s", foundService.Namespace, foundService.Name)
		foundSvc = foundService
	}

	if ing, exist, err := utils.checkIngress(instance); err != nil {
		return err
	} else if ing != nil {
		changed = true
		klog.Infof("udpate ing %s/%s", ing.Namespace, ing.Name)
		if err = controllerutil.SetControllerReference(instance, ing, utils.scheme); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedSetOwnerReference", "failed to set owner reference for ingress %s", err.Error())
			return err
		}

		if err = utils.apply(ing, exist); err != nil {
			utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "FailedApplyIngress", "failed to apply ingress %s", err.Error())
			return err
		}
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "SuccessedApplyIngress", "successed create ingress %s/%s", ing.Namespace, ing.Name)
	} else {
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "IngressExist", "ingres exist already %s/%s-ingress", instance.Namespace, instance.Name)
	}
	var firstPutError error
	if !depIsUpdating && foundDep != nil && *foundDep.Spec.Replicas > 0 && foundSvc != nil && deploymentComplete(foundDep, &(foundDep.Status)) {
		klog.Infof("dep complete %s/%s, will check config", foundDep.Namespace, foundDep.Name)
		kcNamespacedName := types.NamespacedName{Namespace: instance.Namespace, Name: instance.Name}
		newConnectorStatus := make([]kafkaconnectv1alpha1.KafkaConnectorStatus, len(instance.Spec.KafkaConnectorsSpec.Configs))
		oldConnectorStatus := instance.Status.KafkaConnectorStatus
		var connectors []string
		if !instance.Spec.KafkaConnectorsSpec.KeepUnknownConnectors {
			c, err := utils.kcc.GetAllConnectors(kcNamespacedName, instance.Spec.KafkaConnectRestAPIPort)
			if err != nil {
				klog.Error("unable to connect to kafka connect rest api ", err)
				utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "UnableToConnectRestAPI", "unable to connect to kafka connect rest api %s", err.Error())
				return err
			}
			connectors = c
		}

		for i, config := range instance.Spec.KafkaConnectorsSpec.Configs {
			foundOldStatus := false
			for _, cStatus := range oldConnectorStatus {
				if cStatus.Name == config.Name {
					foundOldStatus = true
					newConnectorStatus[i] = cStatus
					break
				}
			}
			if !foundOldStatus {
				newConnectorStatus[i] = kafkaconnectv1alpha1.KafkaConnectorStatus{
					Name:   config.Name,
					TaskNb: int32(0),
					Error:  "",
				}
			}
			if !instance.Spec.KafkaConnectorsSpec.KeepUnknownConnectors && connectors != nil {
				for idx := range connectors {
					if connectors[idx] == config.Name {
						connectors[idx] = connectors[len(connectors)-1]
						connectors[len(connectors)-1] = ""
						connectors = connectors[:len(connectors)-1]
						break
					}
				}
			}
			//get existing config from kafka connect cluster
			utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "CheckingConnectorConfig", "checking connector config for %s", config.Name)
			if *config.TasksMax == 0 && newConnectorStatus[i].Error != "" {
				//TODO should i delete old connector????????
				utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "WrongTaskMax", "wrong value tasksmax for %s", config.Name)
				continue
			}

			foundConfig, err := utils.kcc.GetKafkaConnectConfig(config.Name, instance.Spec.KafkaConnectRestAPIPort, kcNamespacedName)
			if err != nil {
				return err
			}
			if errorCode, ok := foundConfig["error_code"]; ok {
				klog.Warning("KafkaConnect REST Api not available error code ", errorCode)
				if message, ok := foundConfig["message"]; ok {
					klog.Warning("error message : ", message)
				}
				klog.Warningf("KafkaConnect REST Api not available error code %v", errorCode)
				utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "RESTApiUnavailable", "KafkaConnect REST Api not available error code %v", errorCode)
				//return error so retry later
				return e.New("RESTApi not available")
			}
			// get config from url
			expectedConfig, err := kafkaconnectclient.GetKafkaConnectConfig(utils.corev1Itf.ConfigMaps(instance.Namespace), config)
			if err != nil {
				return err
			}
			// replace tasks max with the nb from status. this value is updated by autoscaler
			expectedConfig["tasks.max"] = strconv.Itoa(int(*config.TasksMax))
			expectedConfig["name"] = config.Name
			if !apiequality.Semantic.DeepEqual(foundConfig, expectedConfig) {
				changed = true
				utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "UpdatingConnector", "updating connector %s", config.Name)
				klog.Infof("config is diff, will update config new : %+v", expectedConfig)
				klog.Infof("config is diff, will update config old : %+v", foundConfig)

				expectedConfigString, err := json.Marshal(expectedConfig)
				if err != nil {
					klog.Error("unable to marshal config ", err)
				} else if err = utils.kcc.PutKafkaConnectConfig(config.Name, instance.Spec.KafkaConnectRestAPIPort, kcNamespacedName, expectedConfigString); err != nil {
					newConnectorStatus[i].Error = err.Error()
					firstPutError = err
				} else {
					newConnectorStatus[i].TaskNb = *config.TasksMax
				}

			} else {
				utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "SameConnector", "same connector config %s", config.Name)
				klog.Info("config is same, won't update config")
				newConnectorStatus[i].TaskNb = *config.TasksMax
			}
		}

		if !instance.Spec.KafkaConnectorsSpec.KeepUnknownConnectors && connectors != nil {
			for _, c := range connectors {
				err := utils.kcc.DeleteConnector(c, instance.Spec.KafkaConnectRestAPIPort, kcNamespacedName)
				if err != nil {
					klog.Errorf("unable to delete connector %s with error %s", c, err.Error())
					utils.eventRecorder.Eventf(instance, v1.EventTypeWarning, "CannotDelConnector", "unable to delete connector %s with error %s", c, err.Error())

					//Do not return with error, will retry later
				}
			}
		}
		instance.Status.KafkaConnectorStatus = newConnectorStatus
	}

	if !changed && deploymentComplete(foundDep, &(foundDep.Status)) && foundDep != nil && instance.Status.PodNb < *foundDep.Spec.Replicas {
		changed = true
		err := utils.ScaleDeployment(instance, foundDep)
		if err != nil {
			utilruntime.HandleError(err)
			return err
		}
		utils.eventRecorder.Eventf(instance, v1.EventTypeNormal, "ScaleUpDeployment", "deployment exist already need to scale up %s/%s", foundDep.Namespace, foundDep.Name)
	}
	if !changed && foundDep != nil && deploymentComplete(foundDep, &(foundDep.Status)) {
		utils.checkUpdatingStatus(instance, false)
	} else {
		utils.checkUpdatingStatus(instance, true)
	}

	currentInstance := &kafkaconnectv1alpha1.KafkaConnect{}
	err := utils.client.Get(context.TODO(), types.NamespacedName{Name: instance.Name, Namespace: instance.Namespace}, currentInstance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return nil
		}
		// Error reading the object - requeue the request.
		return err
	}
	//the instance has been updated during this reconcil,
	//we will do nothing and wait for the next reconcil
	if !apiequality.Semantic.DeepEqual(oriInstance, currentInstance) {
		klog.Warningf("instance %s:%s has been updated during the reconcilation, will apply the status to the new instance",
			currentInstance.Namespace, currentInstance.Name)
		klog.Warningf("the old instance is : %+v", oriInstance)
		klog.Warningf("the new instance is : %+v", currentInstance)
		currentInstance.Status = instance.Status
		if err := utils.client.Status().Update(context.TODO(), currentInstance); err != nil {
			utilruntime.HandleError(err)
		}

	} else if !apiequality.Semantic.DeepEqual(oriInstance.Status, instance.Status) {
		if err := utils.client.Status().Update(context.TODO(), instance); err != nil {
			utilruntime.HandleError(err)
		}
	}
	return firstPutError
}

func deploymentComplete(deployment *appsv1.Deployment, newStatus *appsv1.DeploymentStatus) bool {
	return newStatus.UpdatedReplicas == *(deployment.Spec.Replicas) &&
		newStatus.Replicas == *(deployment.Spec.Replicas) &&
		newStatus.AvailableReplicas == *(deployment.Spec.Replicas) &&
		newStatus.ObservedGeneration >= deployment.Generation
}

func (utils *Utils) apply(obj runtime.Object, exist bool) error {
	if obj != nil {
		if exist {
			return utils.client.Update(context.TODO(), obj)
		}
		return utils.client.Create(context.TODO(), obj)

	}
	return nil

}

// checkDeployment check if we need to create a new deployment or not, if only replicas is different, just scale old one
func (utils *Utils) checkDeployment(instance *kafkaconnectv1alpha1.KafkaConnect) (*appsv1.Deployment, *appsv1.Deployment, error) {
	foundDeployment := &appsv1.Deployment{}
	podLabels := GeneratePodLabels(*instance)
	podTemplate := corev1.PodTemplateSpec{
		Spec: instance.Spec.PodSpec,
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
			Labels:    podLabels,
		},
	}
	tasksTotal := utils.getTotalTaskNb(instance)
	tpp := int32(1)
	if instance.Spec.KafkaConnectorsSpec.TaskPerPod != nil {
		tpp = *instance.Spec.KafkaConnectorsSpec.TaskPerPod
	}
	// only 1 pod if tpp <= 0
	instance.Status.PodNb = int32(1)
	if tpp > 0 {
		instance.Status.PodNb = tasksTotal / tpp
		if tasksTotal%tpp > 0 {
			instance.Status.PodNb = instance.Status.PodNb + 1
		}
	} else if instance.Spec.KafkaConnectorsSpec.InitDeploymentReplicas != nil && *instance.Spec.KafkaConnectorsSpec.InitDeploymentReplicas > 0 {
		instance.Status.PodNb = int32(1)
	} else {
		klog.Warning("taskPerPod is less than 0 and initPodReplicas is not validate value, we create deployment with 1 pod")
		utils.eventRecorder.Event(instance, v1.EventTypeWarning, "UnableToCalculReplicas", "taskPerPod is less than 0 and initPodReplicas is not validate value, we create deployment with 1 pod")
	}
	expectedDeployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      instance.Name,
			Namespace: instance.Namespace,
			Labels:    instance.Labels,
		},
		Spec: appsv1.DeploymentSpec{
			//Replicas: &instance.Status.PodNb,
			Selector: &metav1.LabelSelector{
				MatchLabels: podLabels,
			},
			Template: podTemplate,
		},
	}
	exist := true

	err := utils.client.Get(context.TODO(), types.NamespacedName{Name: expectedDeployment.Name, Namespace: expectedDeployment.Namespace}, foundDeployment)
	//if no deployment or deployement is different
	needUpdateDeployment := false
	if err != nil {
		if errors.IsNotFound(err) {
			needUpdateDeployment = true
			exist = false
		} else {
			return nil, nil, err
		}
	}
	if !needUpdateDeployment {
		annotationDeployment := &appsv1.Deployment{}
		needUpdateDeployment = !checkSame(&foundDeployment.ObjectMeta, expectedDeployment, annotationDeployment, encodedDeploymentAnnotation)
	}
	if needUpdateDeployment {
		err = udpateObjectAnnotation(expectedDeployment, &(expectedDeployment.ObjectMeta), encodedDeploymentAnnotation)
		if err != nil {
			return nil, nil, err
		}
		expectedDeployment.Spec.Replicas = &instance.Status.PodNb
		if exist {
			return expectedDeployment, foundDeployment, nil
		}
		return expectedDeployment, nil, nil

	}
	if tpp <= 0 {
		instance.Status.PodNb = *foundDeployment.Spec.Replicas
		return nil, foundDeployment, nil
	}
	return nil, foundDeployment, nil
}

// ScaleDeployment will scale dep according to instance.Status.PodNb
func (utils *Utils) ScaleDeployment(instance *kafkaconnectv1alpha1.KafkaConnect, dep *appsv1.Deployment) error {
	//if replicas is different we need to only scale
	if instance.Status.PodNb != *dep.Spec.Replicas {

		gvk := dep.GroupVersionKind()
		mapping, err := utils.restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			klog.Error(err)
			utils.eventRecorder.Eventf(dep, v1.EventTypeWarning, "UnableToFindRESTMapping", "cannot find RESTMapping for %s", gvk.String())
			return err
		}
		gr := mapping.Resource.GroupResource()
		scale, err := utils.scaleClient.Scales(dep.Namespace).Get(gr, dep.Name)
		if err != nil {
			klog.Error(err)
			utils.eventRecorder.Eventf(dep, v1.EventTypeWarning, "UnableToGetScale", "cannot get Scale for deployement %s/%s", dep.GetNamespace(), dep.GetName())
			return err

		}
		scale.Spec.Replicas = instance.Status.PodNb
		scale, err = utils.scaleClient.Scales(dep.Namespace).Update(gr, scale)
		if err != nil {
			klog.Error(err)
			utils.eventRecorder.Eventf(dep, v1.EventTypeWarning, "UnableToUpdateScale", "cannot update Scale for deployement %s/%s", dep.GetNamespace(), dep.GetName())
			return err
		}
	}
	return nil
}

//GeneratePodLabels create label to select pod
func GeneratePodLabels(instance kafkaconnectv1alpha1.KafkaConnect) map[string]string {
	podLabels := map[string]string{
		"name": instance.Name,
		"type": "kafkaConnect",
	}
	return podLabels
}

// GetSvcName generate the service name from the KafkaConnect
func GetSvcName(kcName string) string {
	return fmt.Sprintf("%s-service", kcName)
}

// checkService check if we need to create a new service or not
func (utils *Utils) checkService(instance *kafkaconnectv1alpha1.KafkaConnect) (*corev1.Service, *corev1.Service, error) {
	foundService := &corev1.Service{}
	port := instance.Spec.KafkaConnectRestAPIPort
	// generate pod label
	podLabels := GeneratePodLabels(*instance)
	expectedService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetSvcName(instance.Name),
			Namespace: instance.Namespace,
			Labels: map[string]string{
				"app":  "kafkaconnect",
				"type": "service",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector:  podLabels,
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Name:       "kafkaconnect-rest",
					Port:       port,
					TargetPort: intstr.FromInt(int(port)),
				},
			},
		},
	}
	exist := true
	err := utils.client.Get(context.TODO(), types.NamespacedName{Name: expectedService.Name, Namespace: expectedService.Namespace}, foundService)
	//if no service or service is different

	needUpdateService := false
	if err != nil {
		if errors.IsNotFound(err) {
			needUpdateService = true
			exist = false
		} else {
			return nil, nil, err
		}
	}
	if !needUpdateService {
		annotationService := &corev1.Service{}
		needUpdateService = !checkSame(&foundService.ObjectMeta, expectedService, annotationService, encodedServiceAnnotation)

	}
	if needUpdateService {
		err = udpateObjectAnnotation(expectedService, &(expectedService.ObjectMeta), encodedServiceAnnotation)

		if err != nil {
			return nil, nil, err
		}

		if exist {
			expectedService.ResourceVersion = foundService.ResourceVersion
			return expectedService, foundService, nil
		}

		return expectedService, nil, nil
	}
	return nil, foundService, nil
}

// checkIngress check if we need to create a new ingress or not
func (utils *Utils) checkIngress(instance *kafkaconnectv1alpha1.KafkaConnect) (*extensions.Ingress, bool, error) {
	if instance.Spec.IngressSpec == nil {
		return nil, true, nil
	}

	var expectedIngress *extensions.Ingress
	spec := *instance.Spec.IngressSpec
	if spec.Style != nil && *spec.Style == kafkaconnectv1alpha1.DomainStyle {
		expectedIngress = &extensions.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-ingress", instance.Name),
				Namespace: instance.Namespace,
				Labels: map[string]string{
					"app":  "kafkaconnect",
					"type": "service",
				},
			},
			Spec: extensions.IngressSpec{
				Rules: []extensions.IngressRule{
					{
						Host: fmt.Sprintf("%s.%s", instance.Name, instance.Spec.IngressSpec.ParentDomain),
						IngressRuleValue: extensions.IngressRuleValue{
							HTTP: &extensions.HTTPIngressRuleValue{
								Paths: []extensions.HTTPIngressPath{
									{
										Backend: extensions.IngressBackend{
											ServiceName: fmt.Sprintf("%s-service", instance.Name),
											ServicePort: intstr.FromInt(8083),
										},
									},
								},
							},
						},
					},
				},
			},
		}
	} else {
		expectedIngress = &extensions.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-ingress", instance.Name),
				Namespace: instance.Namespace,
				Labels: map[string]string{
					"app":  "kafkaconnect",
					"type": "service",
				},
			},
			Spec: extensions.IngressSpec{
				Rules: []extensions.IngressRule{
					{
						IngressRuleValue: extensions.IngressRuleValue{
							HTTP: &extensions.HTTPIngressRuleValue{
								Paths: []extensions.HTTPIngressPath{
									{
										Path: fmt.Sprintf("/%s", instance.Name),
										Backend: extensions.IngressBackend{
											ServiceName: fmt.Sprintf("%s-service", instance.Name),
											ServicePort: intstr.FromInt(8083),
										},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	klog.Info("check ingress")

	foundIngress := &extensions.Ingress{}
	err := utils.client.Get(context.TODO(), types.NamespacedName{Name: expectedIngress.Name, Namespace: expectedIngress.Namespace}, foundIngress)
	//if no ingress or ingress is different
	exist := true
	needUpdateIngress := false
	if err != nil {
		if errors.IsNotFound(err) {

			klog.Info("ingress not found, create new")
			needUpdateIngress = true
			exist = false
		} else {
			return nil, false, err
		}
	}
	if !needUpdateIngress {

		klog.Info("ingress found compare annotation")
		annotationIngress := &extensions.Ingress{}
		needUpdateIngress = !checkSame(&foundIngress.ObjectMeta, expectedIngress, annotationIngress, encodedIngressAnnotation)
	}
	if needUpdateIngress {

		klog.Info("need to update")
		err = udpateObjectAnnotation(expectedIngress, &(expectedIngress.ObjectMeta), encodedIngressAnnotation)
		if err != nil {
			return nil, false, err
		}
		return expectedIngress, exist, nil
	}
	return nil, true, nil
}

func checkSame(annotationObj *metav1.ObjectMeta, exptectedObject interface{}, jsonEncodedObj interface{}, annotationName string) bool {
	if annotation, ok := annotationObj.Annotations[annotationName]; ok {
		json.Unmarshal([]byte(annotation), jsonEncodedObj)
		if !apiequality.Semantic.DeepEqual(jsonEncodedObj, exptectedObject) {
			return false
		}
	} else {
		return false
	}
	return true
}
func udpateObjectAnnotation(exptectedObject interface{}, meta *metav1.ObjectMeta, annotationName string) error {
	jsonEncoded, err := json.Marshal(exptectedObject)
	if err != nil {
		return err
	}
	meta.Annotations = map[string]string{}
	meta.Annotations[annotationName] = string(jsonEncoded)
	return nil
}
