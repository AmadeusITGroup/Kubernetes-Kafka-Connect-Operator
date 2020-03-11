# Developer Guide

## Prerequisites
- [git](https://git-scm.com/downloads)
- [go](https://golang.org/dl/) version v1.13+.
- [docker](https://docs.docker.com/install/) version 17.03+.
- [operator-sdk](https://github.com/operator-framework/operator-sdk) version v0.13.0+.
- [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) version v1.12.0+.
- Access to a Kubernetes v1.12.0+ cluster.

## Build the Operator

In case you want to build the operator from the source code, e.g., to test a fix or a feature you write, you can do so following the instructions below.

The easiest way to build the operator is by using operator-sdk cli

```sh
$ docker build -t <image-tag> -f build/Dockerfile .
```

If you'd like to test/build the spark-operator locally, follow the instructions below:

```sh
$ mkdir -p $GOPATH/src/github.com/AmadeusITGroup
$ cd $GOPATH/src/github.com/AmadeusITGroup
$ git clone git@github.com:amadeusitgroup/kubernetes-kafka-connect-operator.git
$ cd Kubernetes-Kafka-Connect-Operator
$ go test ./...
$ docker build -t <image-tag> -f build/Dockerfile .
```
### Update the auto-generated code

To update the auto-generated CRD definitions, run the following command:
```sh
$ cd $GOPATH/src/github.com/amadeusitgroup/kubernetes-kafka-connect-operator
$ operator-sdk generate crds
```

If you want to update the auto-generated code like `deepcopy funtion`, `clientset`, `lister`, `informer`, run the following commands to get the required Kubernetes code generators:
```sh
$ go get -u k8s.io/code-generator
```
Then run the following commands to update the code 
```sh
$ $GOPATH/src/github.com/amadeusitgroup/kubernetes-kafka-connect-operator/hack/update-codegen.sh
```
