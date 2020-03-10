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

package kafkaconnectautoscaler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafkaconnectv1alpha1 "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/apis/kafkaconnect/v1alpha1"
	"github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/controller/kafkaconnect"
	clientset "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/generated/clientset/versioned"
	informersfactory "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/generated/informers/externalversions"
	listers "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/generated/listers/kafkaconnect/v1alpha1"
	"github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/kafkaconnectclient"
	"github.com/operator-framework/operator-sdk/pkg/k8sutil"
	autoscalingv2 "k8s.io/api/autoscaling/v2beta2"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	k8sinformers "k8s.io/client-go/informers"
	k8sclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"k8s.io/kubernetes/pkg/controller/podautoscaler"
	"k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
	resourceclient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/custom_metrics"
	"k8s.io/metrics/pkg/client/external_metrics"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	kafkaConnectKind             = "KafkaConnect"
	defaultStabilisationDuration = time.Minute * 5
)

var (
	scaleUpLimitFactor  = 2.0
	scaleUpLimitMinimum = 4.0
	watchNameSpace      = ""
	stopCh              = setupSignalHandler()
)

func setupSignalHandler() (stopCh <-chan struct{}) {
	onlyOneSignalHandler := make(chan struct{})
	close(onlyOneSignalHandler) // panics when called twice
	shutdownSignals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new KafkaConnectAutoScaler Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	var err error
	if watchNameSpace, err = k8sutil.GetWatchNamespace(); err != nil {
		klog.Error(err, "error when get namespace")
		return err
	}

	klog.Infof("watch namespace %s", watchNameSpace)
	r, err := newReconciler(mgr)
	if err != nil {
		return err
	}
	return add(mgr, r)
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) (*ReconcileKafkaConnectAutoScaler, error) {
	cfg := mgr.GetConfig()
	clientSet := clientset.NewForConfigOrDie(cfg)
	k8sclientSet := k8sclientset.NewForConfigOrDie(cfg)
	informerFactory := informersfactory.NewSharedInformerFactoryWithOptions(clientSet, time.Second*30, informersfactory.WithNamespace(watchNameSpace))

	k8sInformerFactory := k8sinformers.NewSharedInformerFactoryWithOptions(k8sclientSet, time.Second*30, k8sinformers.WithNamespace(watchNameSpace))

	scalerInformer := informerFactory.Kafkaconnect().V1alpha1().KafkaConnectAutoScalers()

	kcInformer := informerFactory.Kafkaconnect().V1alpha1().KafkaConnects()

	apiVersionsGetter := custom_metrics.NewAvailableAPIsGetter(clientSet.Discovery())
	// invalidate the discovery information roughly once per resync interval our API
	// information is *at most* two resync intervals old.
	go custom_metrics.PeriodicallyInvalidate(
		apiVersionsGetter,
		time.Minute*5,
		stopCh)

	metricsClient := metrics.NewRESTMetricsClient(
		resourceclient.NewForConfigOrDie(cfg),
		custom_metrics.NewForConfig(cfg, mgr.GetRESTMapper(), apiVersionsGetter),
		external_metrics.NewForConfigOrDie(cfg),
	)

	podInformer := k8sInformerFactory.Core().V1().Pods()

	replicaCalc := podautoscaler.NewReplicaCalculator(
		metricsClient,
		podInformer.Lister(),
		// TODO export all below as a config
		0.2,
		time.Minute*5,
		time.Second*30,
	)

	r := &ReconcileKafkaConnectAutoScaler{
		client:         mgr.GetClient(),
		autoScalerList: scalerInformer.Lister(),
		kcList:         kcInformer.Lister(),
		replicaCalc:    replicaCalc,
		eventRecorder:  mgr.GetEventRecorderFor("kafkaconnect-autoscaler"),
	}
	informerFactory.Start(stopCh)
	k8sInformerFactory.Start(stopCh)
	klog.Info("wait for cache in scaler")
	// wait cache to be ready
	if !cache.WaitForCacheSync(stopCh, scalerInformer.Informer().HasSynced, kcInformer.Informer().HasSynced, podInformer.Informer().HasSynced) {
		return nil, errors.New("cannot sync the informer cache for kc autoscaler")
	}
	return r, nil
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r *ReconcileKafkaConnectAutoScaler) error {
	// Create a new controller
	c, err := controller.New("kafkaconnectautoscaler-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource KafkaConnectAutoScaler
	err = c.Watch(&source.Kind{Type: &kafkaconnectv1alpha1.KafkaConnectAutoScaler{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileKafkaConnectAutoScaler implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileKafkaConnectAutoScaler{}

// ReconcileKafkaConnectAutoScaler reconciles a KafkaConnectAutoScaler object
type ReconcileKafkaConnectAutoScaler struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client         client.Client
	autoScalerList listers.KafkaConnectAutoScalerLister
	kcList         listers.KafkaConnectLister
	replicaCalc    *podautoscaler.ReplicaCalculator
	eventRecorder  record.EventRecorder
}

type timestampedRecommendation struct {
	recommendation int32
	timestamp      time.Time
}

// Reconcile reads that state of the cluster for a KafkaConnectAutoScaler object and makes changes based on the state read
// and what is in the KafkaConnectAutoScaler.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileKafkaConnectAutoScaler) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	// reqLogger.Info("Reconciling KafkaConnectAutoScaler")
	klog.Infof("start autoscaler %s", request.NamespacedName.String())
	// Fetch the KafkaConnectAutoScaler instance
	instance := &kafkaconnectv1alpha1.KafkaConnectAutoScaler{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	if instance.DeletionTimestamp != nil {
		//killing
		return reconcile.Result{}, nil
	}
	if val, ok := instance.Labels["KafkaConnect"]; ok && val == instance.Spec.KafkaConnectorScaleTargetRef.Name {

		wait := r.reconcileKey(request.NamespacedName, instance)

		klog.Infof("retry in %v", wait)
		// reconcile every 5 sec
		return reconcile.Result{
			RequeueAfter: wait,
		}, nil
	}
	if instance.Labels == nil {
		instance.Labels = make(map[string]string)
	}
	instance.Labels["KafkaConnect"] = instance.Spec.KafkaConnectorScaleTargetRef.Name
	r.client.Update(context.TODO(), instance)
	return reconcile.Result{}, nil
}

func (r *ReconcileKafkaConnectAutoScaler) reconcileKey(namespacedName types.NamespacedName, kcScaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler) time.Duration {
	klog.Infof("reconsile autocaler for %s:%s", namespacedName.Namespace, namespacedName.Name)
	autoscalerStatusOriginal := kcScaler.Status.DeepCopy()
	kafkaConnectCache, err := r.kcList.KafkaConnects(namespacedName.Namespace).Get(kcScaler.Spec.KafkaConnectorScaleTargetRef.Name)
	if err != nil {
		r.eventRecorder.Event(kcScaler, v1.EventTypeWarning, "FailedGetKafkaConnect", err.Error())
		setCondition(kcScaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetKafkaConnect", "the kafkaConnectAutoScaler controller was unable to get the target's current KafkaConnect: %v", err)
		r.updateStatusIfNeeded(autoscalerStatusOriginal, kcScaler)
		return time.Second * 5
	}

	kafkaConnect := kafkaConnectCache.DeepCopy()
	scaleWaitDuration := defaultStabilisationDuration
	if kafkaConnect.Spec.ScaleStabilizationSec != nil {
		scaleWaitDuration = time.Second * time.Duration(*kafkaConnect.Spec.ScaleStabilizationSec)
	}

	for _, config := range kafkaConnect.Spec.KafkaConnectorsSpec.Configs {
		if config.TasksMax == nil {
			// kafka connect not yet init
			return scaleWaitDuration
		}
	}
	orignalKCSpec := kafkaConnectCache.Spec
	if kafkaConnectCache.Status == nil || kafkaConnectCache.Status.Updating {
		return scaleWaitDuration
	} else if time.Now().Sub(kafkaConnect.Status.LastScaleTime.Time) <= scaleWaitDuration {
		return scaleWaitDuration - time.Now().Sub(kafkaConnect.Status.LastScaleTime.Time)
	}
	klog.Info("parse label selector")
	selector, err := labels.Parse(fmt.Sprintf("KafkaConnect=%s", kafkaConnect.Name))
	if err != nil {
		r.eventRecorder.Event(kcScaler, v1.EventTypeWarning, "CannotParseSelector", err.Error())
		setCondition(kcScaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "CannotParseSelector", "selector cannot be parsed: KafkaConnect=%s", kafkaConnect.Name)
		r.updateStatusIfNeeded(autoscalerStatusOriginal, kcScaler)
		return time.Second * 5
	}
	autoscalersCache, err := r.autoScalerList.List(selector)
	if err != nil {
		r.eventRecorder.Event(kcScaler, v1.EventTypeWarning, "CannotListAutoScalers", err.Error())
		setCondition(kcScaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "CannotListAutoScalers", "cannot list autoscalers : KafkaConnect=%s", kafkaConnect.Name)
		r.updateStatusIfNeeded(autoscalerStatusOriginal, kcScaler)
		return time.Second * 5
	}
	klog.Infof("find %d kafkaconnect autoscaler", len(autoscalersCache))
	var wg sync.WaitGroup
	autoscalers := make([]*kafkaconnectv1alpha1.KafkaConnectAutoScaler, len(autoscalersCache))
	autoscalersOriStatus := make([]*kafkaconnectv1alpha1.KafkaConnectAutoScalerStatus, len(autoscalersCache))
	for i, autoScalerCache := range autoscalersCache {

		klog.Infof("start process kafka connect autoscaler %s/%s", autoScalerCache.Namespace, autoScalerCache.Name)
		// make a copy so that we never mutate the shared informer cache (conversion can mutate the object)
		autoscalersOriStatus[i] = autoScalerCache.Status.DeepCopy()
		autoscalers[i] = autoScalerCache.DeepCopy()
		wg.Add(1)
		go r.reconcileAutoscaler(&wg, kafkaConnect, autoscalers[i], namespacedName)
	}
	wg.Wait()
	klog.Infof("connectors :%d", *kafkaConnect.Spec.KafkaConnectorsSpec.Configs[0].TasksMax)
	if apiequality.Semantic.DeepEqual(orignalKCSpec, &kafkaConnect.Spec) {
		klog.Info("same kafka connect no need to update")
		return time.Second * 5
	}
	klog.Infof("udpate kafka connect %s/%s", kafkaConnect.Namespace, kafkaConnect.Name)
	err = r.client.Update(context.TODO(), kafkaConnect)
	if err != nil {
		r.eventRecorder.Eventf(kafkaConnect, v1.EventTypeWarning, "FailedUpdateKafkaConnect", "rescale reqeust by autoscaler but update failed: %v", err.Error())
		for i, autoScaler := range autoscalers {
			setCondition(autoScaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedUpdateScale", "the KafkaConnect was unable to be updated: %v", err)
			if err := r.updateStatusIfNeeded(autoscalersOriStatus[i], autoScaler); err != nil {
				utilruntime.HandleError(err)
			}
		}

	} else {
		r.eventRecorder.Eventf(kafkaConnect, v1.EventTypeNormal, "SuccessfulRescale", "replica/tasks successfully updated in KafkaConnect %s/%s", kafkaConnect.Namespace, kafkaConnect.Name)
		for _, autoScaler := range autoscalers {
			setCondition(autoScaler, autoscalingv2.AbleToScale, v1.ConditionTrue, "SucceededRescale", "the KafkaConnect controller was able to update the target scale")
			klog.Infof("Successful update KafkaConnect of %s for autoscale", kafkaConnect.Name)
		}
	}

	return scaleWaitDuration

}

// setCondition sets the specific condition type on the given HPA to the specified value with the given reason
// and message.  The message and args are treated like a format string.  The condition will be added if it is
// not present.
func setCondition(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType, status v1.ConditionStatus, reason, message string, args ...interface{}) {
	autoscaler.Status.Conditions = setConditionInList(autoscaler.Status.Conditions, conditionType, status, reason, message, args...)
}

// setConditionInList sets the specific condition type on the given HPA to the specified value with the given
// reason and message.  The message and args are treated like a format string.  The condition will be added if
// it is not present.  The new list will be returned.
func setConditionInList(inputList []autoscalingv2.HorizontalPodAutoscalerCondition, conditionType autoscalingv2.HorizontalPodAutoscalerConditionType, status v1.ConditionStatus, reason, message string, args ...interface{}) []autoscalingv2.HorizontalPodAutoscalerCondition {
	resList := inputList
	var existingCond *autoscalingv2.HorizontalPodAutoscalerCondition
	for i, condition := range resList {
		if condition.Type == conditionType {
			// can't take a pointer to an iteration variable
			existingCond = &resList[i]
			break
		}
	}

	if existingCond == nil {
		resList = append(resList, autoscalingv2.HorizontalPodAutoscalerCondition{
			Type: conditionType,
		})
		existingCond = &resList[len(resList)-1]
	}

	if existingCond.Status != status {
		existingCond.LastTransitionTime = metav1.Now()
	}

	existingCond.Status = status
	existingCond.Reason = reason
	existingCond.Message = fmt.Sprintf(message, args...)

	return resList
}

func (r *ReconcileKafkaConnectAutoScaler) getUnableComputeReplicaCountCondition(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, reason string, err error) (condition autoscalingv2.HorizontalPodAutoscalerCondition) {
	r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, reason, err.Error())
	return autoscalingv2.HorizontalPodAutoscalerCondition{
		Type:    autoscalingv2.ScalingActive,
		Status:  v1.ConditionFalse,
		Reason:  reason,
		Message: fmt.Sprintf("the HPA was unable to compute the replica count: %v", err),
	}
}

// computeStatusForPodsMetric computes the desired number of replicas for the specified metric of type PodsMetricSourceType.
func (r *ReconcileKafkaConnectAutoScaler) computeStatusForPodsMetric(currentReplicas int32, metricSpec autoscalingv2.MetricSpec, autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, selector labels.Selector, status *autoscalingv2.MetricStatus, metricSelector labels.Selector) (replicaCountProposal int32, timestampProposal time.Time, metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	replicaCountProposal, utilizationProposal, timestampProposal, err := r.replicaCalc.GetMetricReplicas(currentReplicas, metricSpec.Pods.Target.AverageValue.MilliValue(), metricSpec.Pods.Metric.Name, autoscaler.Namespace, selector, metricSelector)
	if err != nil {
		condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetPodsMetric", err)
		return 0, timestampProposal, "", condition, err
	}
	*status = autoscalingv2.MetricStatus{
		Type: autoscalingv2.PodsMetricSourceType,
		Pods: &autoscalingv2.PodsMetricStatus{
			Metric: autoscalingv2.MetricIdentifier{
				Name:     metricSpec.Pods.Metric.Name,
				Selector: metricSpec.Pods.Metric.Selector,
			},
			Current: autoscalingv2.MetricValueStatus{
				AverageValue: resource.NewMilliQuantity(utilizationProposal, resource.DecimalSI),
			},
		},
	}

	return replicaCountProposal, timestampProposal, fmt.Sprintf("pods metric %s", metricSpec.Pods.Metric.Name), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
}

// computeStatusForObjectMetric computes the desired number of replicas for the specified metric of type ObjectMetricSourceType.
func (r *ReconcileKafkaConnectAutoScaler) computeStatusForObjectMetric(specReplicas, statusReplicas int32, metricSpec autoscalingv2.MetricSpec, autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, selector labels.Selector, status *autoscalingv2.MetricStatus, metricSelector labels.Selector) (replicas int32, timestamp time.Time, metricName string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	if metricSpec.Object.Target.Type == autoscalingv2.ValueMetricType {
		replicaCountProposal, utilizationProposal, timestampProposal, err := r.replicaCalc.GetObjectMetricReplicas(specReplicas, metricSpec.Object.Target.Value.MilliValue(), metricSpec.Object.Metric.Name, autoscaler.Namespace, &metricSpec.Object.DescribedObject, selector, metricSelector)
		if err != nil {
			condition := r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetObjectMetric", err)
			klog.Error("cannot get metric ", err)
			return 0, timestampProposal, "", condition, err
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ObjectMetricSourceType,
			Object: &autoscalingv2.ObjectMetricStatus{
				DescribedObject: metricSpec.Object.DescribedObject,
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.Object.Metric.Name,
					Selector: metricSpec.Object.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					Value: resource.NewMilliQuantity(utilizationProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("%s metric %s", metricSpec.Object.DescribedObject.Kind, metricSpec.Object.Metric.Name), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	} else if metricSpec.Object.Target.Type == autoscalingv2.AverageValueMetricType {
		replicaCountProposal, utilizationProposal, timestampProposal, err := r.replicaCalc.GetObjectPerPodMetricReplicas(statusReplicas, metricSpec.Object.Target.AverageValue.MilliValue(), metricSpec.Object.Metric.Name, autoscaler.Namespace, &metricSpec.Object.DescribedObject, metricSelector)
		if err != nil {
			condition := r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetObjectMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s object metric: %v", metricSpec.Object.Metric.Name, err)
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ObjectMetricSourceType,
			Object: &autoscalingv2.ObjectMetricStatus{
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.Object.Metric.Name,
					Selector: metricSpec.Object.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					AverageValue: resource.NewMilliQuantity(utilizationProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.Object.Metric.Name, metricSpec.Object.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	errMsg := "invalid object metric source: neither a value target nor an average value target was set"
	err = fmt.Errorf(errMsg)
	condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetObjectMetric", err)
	return 0, time.Time{}, "", condition, err
}

// computeStatusForResourceMetric computes the desired number of replicas for the specified metric of type ResourceMetricSourceType.
func (r *ReconcileKafkaConnectAutoScaler) computeStatusForResourceMetric(currentReplicas int32, metricSpec autoscalingv2.MetricSpec, autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, timestampProposal time.Time, metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {

	if metricSpec.Resource.Target.AverageValue != nil {
		var rawProposal int64
		replicaCountProposal, rawProposal, timestampProposal, err := r.replicaCalc.GetRawResourceReplicas(currentReplicas, metricSpec.Resource.Target.AverageValue.MilliValue(), metricSpec.Resource.Name, autoscaler.Namespace, selector)
		if err != nil {
			condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetResourceMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s utilization: %v", metricSpec.Resource.Name, err)
		}
		metricNameProposal = fmt.Sprintf("%s resource", metricSpec.Resource.Name)
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricStatus{
				Name: metricSpec.Resource.Name,
				Current: autoscalingv2.MetricValueStatus{
					AverageValue: resource.NewMilliQuantity(rawProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, metricNameProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	if metricSpec.Resource.Target.AverageUtilization == nil {
		errMsg := "invalid resource metric source: neither a utilization target nor a value target was set"
		err = fmt.Errorf(errMsg)
		condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetResourceMetric", err)
		return 0, time.Time{}, "", condition, fmt.Errorf(errMsg)
	}
	targetUtilization := *metricSpec.Resource.Target.AverageUtilization
	var percentageProposal int32
	var rawProposal int64
	replicaCountProposal, percentageProposal, rawProposal, timestampProposal, err = r.replicaCalc.GetResourceReplicas(currentReplicas, targetUtilization, metricSpec.Resource.Name, autoscaler.Namespace, selector)

	if err != nil {
		condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetResourceMetric", err)
		return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s utilization: %v", metricSpec.Resource.Name, err)
	}
	metricNameProposal = fmt.Sprintf("%s resource utilization (percentage of request)", metricSpec.Resource.Name)
	*status = autoscalingv2.MetricStatus{
		Type: autoscalingv2.ResourceMetricSourceType,
		Resource: &autoscalingv2.ResourceMetricStatus{
			Name: metricSpec.Resource.Name,
			Current: autoscalingv2.MetricValueStatus{
				AverageUtilization: &percentageProposal,
				AverageValue:       resource.NewMilliQuantity(rawProposal, resource.DecimalSI),
			},
		},
	}
	return replicaCountProposal, timestampProposal, metricNameProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil

}

// computeStatusForExternalMetric computes the desired number of replicas for the specified metric of type ExternalMetricSourceType.
func (r *ReconcileKafkaConnectAutoScaler) computeStatusForExternalMetric(specReplicas, statusReplicas int32, metricSpec autoscalingv2.MetricSpec, autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, timestampProposal time.Time, metricNameProposal string, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	if metricSpec.External.Target.AverageValue != nil {
		replicaCountProposal, utilizationProposal, timestampProposal, err := r.replicaCalc.GetExternalPerPodMetricReplicas(statusReplicas, metricSpec.External.Target.AverageValue.MilliValue(), metricSpec.External.Metric.Name, autoscaler.Namespace, metricSpec.External.Metric.Selector)
		if err != nil {
			condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetExternalMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get %s external metric: %v", metricSpec.External.Metric.Name, err)
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ExternalMetricSourceType,
			External: &autoscalingv2.ExternalMetricStatus{
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.External.Metric.Name,
					Selector: metricSpec.External.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					AverageValue: resource.NewMilliQuantity(utilizationProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.External.Metric.Name, metricSpec.External.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	if metricSpec.External.Target.Value != nil {
		replicaCountProposal, utilizationProposal, timestampProposal, err := r.replicaCalc.GetExternalMetricReplicas(specReplicas, metricSpec.External.Target.Value.MilliValue(), metricSpec.External.Metric.Name, autoscaler.Namespace, metricSpec.External.Metric.Selector, selector)
		if err != nil {
			condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetExternalMetric", err)
			return 0, time.Time{}, "", condition, fmt.Errorf("failed to get external metric %s: %v", metricSpec.External.Metric.Name, err)
		}
		*status = autoscalingv2.MetricStatus{
			Type: autoscalingv2.ExternalMetricSourceType,
			External: &autoscalingv2.ExternalMetricStatus{
				Metric: autoscalingv2.MetricIdentifier{
					Name:     metricSpec.External.Metric.Name,
					Selector: metricSpec.External.Metric.Selector,
				},
				Current: autoscalingv2.MetricValueStatus{
					Value: resource.NewMilliQuantity(utilizationProposal, resource.DecimalSI),
				},
			},
		}
		return replicaCountProposal, timestampProposal, fmt.Sprintf("external metric %s(%+v)", metricSpec.External.Metric.Name, metricSpec.External.Metric.Selector), autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
	}
	errMsg := "invalid external metric source: neither a value target nor an average value target was set"
	err = fmt.Errorf(errMsg)
	condition = r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetExternalMetric", err)
	return 0, time.Time{}, "", condition, fmt.Errorf(errMsg)
}

// Computes the desired number of replicas for a specific hpa and metric specification,
// returning the metric status and a proposed condition to be set on the HPA object.
func (r *ReconcileKafkaConnectAutoScaler) computeReplicasForMetric(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, spec autoscalingv2.MetricSpec,
	specTask, currentTask int32, selector labels.Selector, status *autoscalingv2.MetricStatus) (replicaCountProposal int32, metricNameProposal string,
	timestampProposal time.Time, condition autoscalingv2.HorizontalPodAutoscalerCondition, err error) {
	klog.Infof("metric type is : %s", spec.Type)
	switch spec.Type {
	case autoscalingv2.ObjectMetricSourceType:
		metricSelector, err := metav1.LabelSelectorAsSelector(spec.Object.Metric.Selector)
		if err != nil {
			condition := r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetObjectMetric", err)
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get object metric value: %v", err)
		}
		klog.Infof("metric selector is : %v", metricSelector)
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = r.computeStatusForObjectMetric(specTask, currentTask, spec, autoscaler, selector, status, metricSelector)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get object metric value: %v", err)
		}
	case autoscalingv2.PodsMetricSourceType:
		metricSelector, err := metav1.LabelSelectorAsSelector(spec.Pods.Metric.Selector)
		if err != nil {
			condition := r.getUnableComputeReplicaCountCondition(autoscaler, "FailedGetPodsMetric", err)
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get pods metric value: %v", err)
		}
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = r.computeStatusForPodsMetric(specTask, spec, autoscaler, selector, status, metricSelector)
		if err != nil {
			return 0, "", time.Time{}, condition, fmt.Errorf("failed to get pods metric value: %v", err)
		}
	case autoscalingv2.ResourceMetricSourceType:
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = r.computeStatusForResourceMetric(specTask, spec, autoscaler, selector, status)

		if err != nil {
			return 0, "", time.Time{}, condition, err
		}
	case autoscalingv2.ExternalMetricSourceType:
		replicaCountProposal, timestampProposal, metricNameProposal, condition, err = r.computeStatusForExternalMetric(specTask, currentTask, spec, autoscaler, selector, status)
		if err != nil {
			return 0, "", time.Time{}, condition, err
		}
	default:
		errMsg := fmt.Sprintf("unknown metric source type %q", string(spec.Type))
		err = fmt.Errorf(errMsg)
		condition := r.getUnableComputeReplicaCountCondition(autoscaler, "InvalidMetricSourceType", err)
		return 0, "", time.Time{}, condition, err
	}
	return replicaCountProposal, metricNameProposal, timestampProposal, autoscalingv2.HorizontalPodAutoscalerCondition{}, nil
}

// computeReplicasForMetrics computes the desired number of replicas for the metric specifications listed in the HPA,
// returning the maximum  of the computed replica counts, a description of the associated metric, and the statuses of
// all metrics computed.
func (r *ReconcileKafkaConnectAutoScaler) computeReplicasForMetrics(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, kafkaConnect kafkaconnectv1alpha1.KafkaConnect,
	metricSpecs []autoscalingv2.MetricSpec, specTask, currentTask int32) (replicas int32, metric string, statuses []autoscalingv2.MetricStatus, timestamp time.Time, err error) {

	klog.Infof("start to compute replicat for autoscaler %s/%s", autoscaler.Namespace, autoscaler.Name)
	label := labels.FormatLabels(kafkaconnect.GeneratePodLabels(kafkaConnect))
	selector, err := labels.Parse(label)
	if err != nil {
		errMsg := fmt.Sprintf("couldn't convert label into a corresponding internal selector object: %v", err)
		r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, "InvalidSelector", errMsg)
		setCondition(autoscaler, autoscalingv2.ScalingActive, v1.ConditionFalse, "InvalidSelector", errMsg)
		return 0, "", nil, time.Time{}, fmt.Errorf(errMsg)
	}

	statuses = make([]autoscalingv2.MetricStatus, len(metricSpecs))

	invalidMetricsCount := 0
	var invalidMetricError error
	var invalidMetricCondition autoscalingv2.HorizontalPodAutoscalerCondition

	for i, metricSpec := range metricSpecs {

		klog.Infof("start to compute replicat for metric %d", i)
		replicaCountProposal, metricNameProposal, timestampProposal, condition, err := r.computeReplicasForMetric(autoscaler, metricSpec, specTask, currentTask, selector, &statuses[i])
		if err != nil {
			if invalidMetricsCount <= 0 {
				invalidMetricCondition = condition
				invalidMetricError = err
			}
			invalidMetricsCount++
		}
		if err == nil && (replicas == 0 || replicaCountProposal > replicas) {
			timestamp = timestampProposal
			replicas = replicaCountProposal
			metric = metricNameProposal
		}
	}

	// If all metrics are invalid return error and set condition on hpa based on first invalid metric.
	if invalidMetricsCount >= len(metricSpecs) {
		setCondition(autoscaler, invalidMetricCondition.Type, invalidMetricCondition.Status, invalidMetricCondition.Reason, invalidMetricCondition.Message)
		return 0, "", statuses, time.Time{}, fmt.Errorf("invalid metrics (%v invalid out of %v), first error is: %v", invalidMetricsCount, len(metricSpecs), invalidMetricError)
	}
	setCondition(autoscaler, autoscalingv2.ScalingActive, v1.ConditionTrue, "ValidMetricFound", "the HPA was able to successfully calculate a replica count from %s", metric)
	return replicas, metric, statuses, timestamp, nil
}

// updateStatusIfNeeded calls updateStatus only if the status of the new HPA is not the same as the old status
func (r *ReconcileKafkaConnectAutoScaler) updateStatusIfNeeded(oldStatus *kafkaconnectv1alpha1.KafkaConnectAutoScalerStatus, newAutoScaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler) error {
	// skip a write if we wouldn't need to update
	if apiequality.Semantic.DeepEqual(oldStatus, &newAutoScaler.Status) {
		return nil
	}
	return r.client.Status().Update(context.TODO(), newAutoScaler)
}

func (r *ReconcileKafkaConnectAutoScaler) reconcileAutoscaler(wg *sync.WaitGroup, kafkaConnect *kafkaconnectv1alpha1.KafkaConnect, autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, namespacedName types.NamespacedName) error {
	defer wg.Done()
	reference := fmt.Sprintf("%s/%s/%s", kafkaConnectKind, namespacedName.Namespace, autoscaler.Spec.KafkaConnectorScaleTargetRef.Name)
	foundReplicas := false
	specTask := int32(0)
	currentTask := int32(0)
	var err error
	var kafkaConnectorSpec *kafkaconnectv1alpha1.KafkaConnectorConfig
	for i := range kafkaConnect.Spec.KafkaConnectorsSpec.Configs {
		if kafkaConnect.Spec.KafkaConnectorsSpec.Configs[i].Name == autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName {
			kafkaConnectorSpec = &(kafkaConnect.Spec.KafkaConnectorsSpec.Configs[i])
			foundReplicas = true
			//client for kafka connect rest api
			kcc := &kafkaconnectclient.KCClient{}
			specTask = *kafkaConnectorSpec.TasksMax
			namespacedName := types.NamespacedName{Namespace: kafkaConnect.Namespace, Name: kafkaConnect.Name}
			currentTask, err = kcc.GetKafkaConnectorRunningTaskNb(autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName, kafkaConnect.Spec.KafkaConnectRestAPIPort, namespacedName)
			klog.Infof("find %d running tasks for connector %s in kafkaconnect %s", currentTask, autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName, namespacedName.String())
			if err != nil {
				r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, "FailedGetKafkaConnector", err.Error())
				setCondition(autoscaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetKafkaConnect", "the kafkaConnectAutoScaler controller was unable to get the target's current KafkaConnector: %s", autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
				return fmt.Errorf("failed to query kafkaConnector rest api for %s: %s", reference, autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
			}
			break
		}
	}

	if !foundReplicas {
		r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, "FailedGetKafkaConnector", err.Error())
		setCondition(autoscaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "FailedGetKafkaConnect", "the kafkaConnectAutoScaler controller was unable to get the target's current KafkaConnector: %s", autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
		return fmt.Errorf("failed to query kafkaConnector subresource for %s: %s", reference, autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
	}
	if currentTask <= 0 {
		r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, "KafkaConnectorZeroReplicas", err.Error())
		setCondition(autoscaler, autoscalingv2.AbleToScale, v1.ConditionFalse, "KafkaConnectorZeroReplicas", "the kafkaConnectAutoScaler controller find the connector with zero replica: %s", autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
		return fmt.Errorf("zero replicat for %s: %s", reference, autoscaler.Spec.KafkaConnectorScaleTargetRef.KafkaConnectorName)
	}
	setCondition(autoscaler, autoscalingv2.AbleToScale, v1.ConditionTrue, "SucceededGetKafkaConnector", "the kafkaConnectAutoScaler controller was able to get the target's current KafkaConnector")

	var (
		metricStatuses        []autoscalingv2.MetricStatus
		metricDesiredReplicas int32
		metricName            string
	)

	desiredReplicas := int32(0)
	rescaleReason := ""

	var minReplicas int32

	if autoscaler.Spec.MinTasks != nil {
		minReplicas = *autoscaler.Spec.MinTasks
	} else {
		// Default value
		minReplicas = 1
	}

	rescale := true

	if currentTask > autoscaler.Spec.MaxTasks {
		rescaleReason = "Current number of replicas above Spec.MaxTasks"
		desiredReplicas = autoscaler.Spec.MaxTasks
	} else if currentTask < minReplicas {
		rescaleReason = "Current number of replicas below Spec.MinTasks"
		desiredReplicas = minReplicas
	} else {
		var metricTimestamp time.Time
		metricDesiredReplicas, metricName, metricStatuses, metricTimestamp, err = r.computeReplicasForMetrics(autoscaler, *kafkaConnect, autoscaler.Spec.Metrics, specTask, currentTask)
		if err != nil {
			r.setCurrentReplicasInStatus(autoscaler, currentTask)
			//TODO put outside
			// if err := r.updateStatusIfNeeded(autoscalerStatusOriginal, autoscaler); err != nil {
			// 	utilruntime.HandleError(err)
			// }
			r.eventRecorder.Event(autoscaler, v1.EventTypeWarning, "FailedComputeMetricsReplicas", err.Error())
			return fmt.Errorf("failed to compute desired number of replicas based on listed metrics for %s: %v", reference, err)
		}

		klog.V(4).Infof("proposing %v desired replicas (based on %s from %s) for %s", metricDesiredReplicas, metricName, metricTimestamp, reference)

		rescaleMetric := ""
		//TODO reveiw these condition
		if metricDesiredReplicas > desiredReplicas {
			desiredReplicas = metricDesiredReplicas
			rescaleMetric = metricName
		}
		if desiredReplicas > specTask {
			rescaleReason = fmt.Sprintf("%s above target", rescaleMetric)
		}
		if desiredReplicas < specTask {
			rescaleReason = "All metrics below target"
		}

		desiredReplicas = r.normalizeDesiredReplicas(autoscaler, specTask, desiredReplicas, minReplicas)
		rescale = desiredReplicas != specTask
	}

	klog.Infof("desiredReplicas %d", desiredReplicas)
	if rescale {
		kafkaConnectorSpec.TasksMax = &desiredReplicas
		setCondition(autoscaler, autoscalingv2.AbleToScale, v1.ConditionTrue, "SucceededFindNewReplica", "the KafkaConnectAutoScaler controller was able to find the target scale to %d", desiredReplicas)
		r.eventRecorder.Eventf(autoscaler, v1.EventTypeNormal, "SucceededFindNewReplica", "New size: %d; reason: %s", desiredReplicas, rescaleReason)
		klog.Infof("Successful find target replica of %s, old size: %d, new size: %d, reason: %s",
			autoscaler.Name, specTask, desiredReplicas, rescaleReason)
	} else {
		klog.V(4).Infof("decided not to scale %s to %v (last scale time was %s)", reference, desiredReplicas, autoscaler.Status.LastScaleTime)
		desiredReplicas = specTask
	}

	r.setStatus(autoscaler, currentTask, desiredReplicas, metricStatuses, rescale)
	return nil
}

// normalizeDesiredReplicas takes the metrics desired replicas value and normalizes it based on the appropriate conditions (i.e. < maxReplicas, >
// minReplicas, etc...)
func (r *ReconcileKafkaConnectAutoScaler) normalizeDesiredReplicas(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, currentReplicas int32, prenormalizedDesiredReplicas int32, minReplicas int32) int32 {

	desiredReplicas, condition, reason := convertDesiredReplicasWithRules(currentReplicas, prenormalizedDesiredReplicas, minReplicas, autoscaler.Spec.MaxTasks)

	if desiredReplicas == prenormalizedDesiredReplicas {
		setCondition(autoscaler, autoscalingv2.ScalingLimited, v1.ConditionFalse, condition, reason)
	} else {
		setCondition(autoscaler, autoscalingv2.ScalingLimited, v1.ConditionTrue, condition, reason)
	}

	return desiredReplicas
}

func calculateScaleUpLimit(currentReplicas int32) int32 {
	return int32(math.Max(scaleUpLimitFactor*float64(currentReplicas), scaleUpLimitMinimum))
}

// convertDesiredReplicas performs the actual normalization, without depending on `HorizontalController` or `HorizontalPodAutoscaler`
func convertDesiredReplicasWithRules(currentReplicas, desiredReplicas, hpaMinReplicas, hpaMaxReplicas int32) (int32, string, string) {

	var minimumAllowedReplicas int32
	var maximumAllowedReplicas int32

	var possibleLimitingCondition string
	var possibleLimitingReason string

	minimumAllowedReplicas = hpaMinReplicas
	possibleLimitingReason = "the desired replica count is less than the minimum replica count"

	// Do not upscale too much to prevent incorrect rapid increase of the number of master replicas caused by
	// bogus CPU usage report from heapster/kubelet (like in issue #32304).
	scaleUpLimit := calculateScaleUpLimit(currentReplicas)

	if hpaMaxReplicas > scaleUpLimit {
		maximumAllowedReplicas = scaleUpLimit

		possibleLimitingCondition = "ScaleUpLimit"
		possibleLimitingReason = "the desired replica count is increasing faster than the maximum scale rate"
	} else {
		maximumAllowedReplicas = hpaMaxReplicas
		possibleLimitingCondition = "TooManyReplicas"
		possibleLimitingReason = "the desired replica count is more than the maximum replica count"
	}

	if desiredReplicas < minimumAllowedReplicas {
		possibleLimitingCondition = "TooFewReplicas"
		possibleLimitingReason = "the desired replica count is less than the minimum replica count"

		return minimumAllowedReplicas, possibleLimitingCondition, possibleLimitingReason
	} else if desiredReplicas > maximumAllowedReplicas {
		return maximumAllowedReplicas, possibleLimitingCondition, possibleLimitingReason
	}

	return desiredReplicas, "DesiredWithinRange", "the desired count is within the acceptable range"
}

// setCurrentReplicasInStatus sets the current replica count in the status of the HPA.
func (r *ReconcileKafkaConnectAutoScaler) setCurrentReplicasInStatus(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, currentReplicas int32) {
	r.setStatus(autoscaler, currentReplicas, autoscaler.Status.DesiredReplicas, autoscaler.Status.CurrentMetrics, false)
}

// setStatus recreates the status of the given HPA, updating the current and
// desired replicas, as well as the metric statuses
func (r *ReconcileKafkaConnectAutoScaler) setStatus(autoscaler *kafkaconnectv1alpha1.KafkaConnectAutoScaler, currentReplicas, desiredReplicas int32, metricStatuses []autoscalingv2.MetricStatus, rescale bool) {

	status := autoscalingv2.HorizontalPodAutoscalerStatus{
		CurrentReplicas: currentReplicas,
		DesiredReplicas: desiredReplicas,
		LastScaleTime:   autoscaler.Status.LastScaleTime,
		CurrentMetrics:  metricStatuses,
		Conditions:      autoscaler.Status.Conditions,
	}
	autoscaler.Status = kafkaconnectv1alpha1.KafkaConnectAutoScalerStatus{HorizontalPodAutoscalerStatus: status}

	if rescale {
		now := metav1.NewTime(time.Now())
		autoscaler.Status.LastScaleTime = &now
	}
}
