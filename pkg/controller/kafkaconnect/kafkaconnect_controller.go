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
	e "errors"
	"flag"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"syscall"
	"time"

	"github.com/operator-framework/operator-sdk/pkg/k8sutil"

	kafkaconnectv1alpha1 "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/apis/kafkaconnect/v1alpha1"
	kafkaconnectcustommetrics "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/custommetrics"
	clientset "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/generated/clientset/versioned"
	informersfactory "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/generated/informers/externalversions"
	"github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/kafkaconnectclient"
	kafkaconnectprovider "github.com/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator/pkg/provider"
	basecmd "github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/cmd"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */
var watchNameSpace string

// Add creates a new KafkaConnect Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	var err error
	if watchNameSpace, err = k8sutil.GetWatchNamespace(); err != nil {
		klog.Error(err, "error when get namespace")
		return err
	}
	return add(mgr, newReconciler(mgr))
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {

	// Create a new controller
	c, err := controller.New("kafkaconnect-controller", mgr, controller.Options{Reconciler: r})

	if err != nil {
		return err
	}

	// Watch for changes to primary resource KafkaConnect
	err = c.Watch(&source.Kind{Type: &kafkaconnectv1alpha1.KafkaConnect{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for changes to secondary resource Deployment and requeue the owner KafkaConnect
	err = c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &kafkaconnectv1alpha1.KafkaConnect{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileKafkaConnect implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileKafkaConnect{}

// ReconcileKafkaConnect reconciles a KafkaConnect object
type ReconcileKafkaConnect struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	scheme        *runtime.Scheme
	client        client.Client
	kcc           kafkaconnectclient.KCClientItf
	eventRecorder record.EventRecorder
	scaleClient   scale.ScalesGetter
	restMapper    meta.RESTMapper
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	eventRec := mgr.GetEventRecorderFor("KafkaConnect")
	cfg := mgr.GetConfig()
	k8sclientSet := kubernetes.NewForConfigOrDie(cfg)
	k8sclientSet.RESTClient()
	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(k8sclientSet.Discovery())
	scaleClient := scale.New(k8sclientSet.RESTClient(), mgr.GetRESTMapper(), dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)

	klog.Info("Starting Kafka connect Custom Metrics Adapter Server ")

	cmd := &Adapter{}

	kubeconfig := flag.Lookup("kubeconfig").Value.(flag.Getter).Get().(string)
	var local = false
	_, err := k8sutil.GetOperatorNamespace()
	if err != nil {
		if err == k8sutil.ErrRunLocal {
			local = true
		} else {
			panic(err)
		}
	}
	if local {
		// If a flag is specified with the config location, use that
		if len(kubeconfig) == 0 {
			if len(os.Getenv("KUBECONFIG")) > 0 {
				kubeconfig = os.Getenv("KUBECONFIG")
			} else if usr, err := user.Current(); err == nil {
				kubeconfig = filepath.Join(usr.HomeDir, ".kube", "config")
			}
		}
		cmd.Flags().Set("lister-kubeconfig", kubeconfig)
		cmd.Flags().Set("authentication-kubeconfig", kubeconfig)
		cmd.Flags().Set("authorization-kubeconfig", kubeconfig)
	}

	//cmd := &basecmd.AdapterBase{}
	cmd.Flags().Set("cert-dir", "/tmp")
	cmd.Flags().Set("secure-port", "6443")
	cmd.Flags().Set("kubelet-insecure-tls", "")
	cmd.Flags().Set("kubelet-preferred-address-types", "InternalIP,ExternalIP,Hostname")
	cmd.Flags().AddGoFlagSet(flag.CommandLine) // make sure we get the klog flags
	cmd.Flags().Parse(os.Args)

	KCprovider := cmd.makeProviderOrDie(mgr)
	cmd.WithCustomMetrics(KCprovider)
	go cmd.Run(wait.NeverStop)

	return &ReconcileKafkaConnect{
		scheme:        mgr.GetScheme(),
		client:        mgr.GetClient(),
		kcc:           &kafkaconnectclient.KCClient{},
		eventRecorder: eventRec,
		scaleClient:   scaleClient,
		restMapper:    mgr.GetRESTMapper(),
	}
}

// Reconcile reads that state of the cluster for a KafkaConnect object and makes changes based on the state read
// and what is in the KafkaConnect.Spec
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileKafkaConnect) Reconcile(request reconcile.Request) (reconcile.Result, error) {

	klog.Info("Reconciling KafkaConnect for ", request.NamespacedName.String())

	// Fetch the KafkaConnect instance
	instance := &kafkaconnectv1alpha1.KafkaConnect{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
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
	utils := Utils{ReconcileKafkaConnect: *r}
	err = utils.CheckGlobalStatus(instance)
	if err != nil {
		klog.Error(err, "error during the check ")
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil

}
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
func (a *Adapter) makeProviderOrDie(mgr manager.Manager) provider.CustomMetricsProvider {
	clientSet := clientset.NewForConfigOrDie(mgr.GetConfig())
	klog.Infof("watch %s", watchNameSpace)
	informerFactory := informersfactory.NewSharedInformerFactoryWithOptions(clientSet, time.Second*30, informersfactory.WithNamespace(watchNameSpace))
	stopCh := setupSignalHandler()
	kcInformer := informerFactory.Kafkaconnect().V1alpha1().KafkaConnects()

	var KCprovider = kafkaconnectprovider.NewKafkaConnectProvider(kcInformer.Lister(), kafkaconnectcustommetrics.NewClient(mgr.GetClient()))
	informerFactory.Start(stopCh)
	// wait cache to be ready
	klog.Info("wait cache to be ready")
	if !cache.WaitForCacheSync(stopCh, kcInformer.Informer().HasSynced) {
		panic(e.New("cannot sync the informer cache for kc autoscaler"))
	}
	klog.Info("cache is ready")
	return KCprovider

}

//Adapter used for kafka custom metrics
type Adapter struct {
	basecmd.AdapterBase
}
