[![Build Status](https://api.travis-ci.org/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator.svg?branch=master)](https://api.travis-ci.org/AmadeusITGroup/Kubernetes-Kafka-Connect-Operator.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/amadeusitgroup/Kubernetes-Kafka-Connect-Operator)](https://goreportcard.com/report/github.com/amadeusitgroup/Kubernetes-Kafka-Connect-Operator)

# Kubernetes-Kafka-Connect-Operator

A kubernetes operator to deploy and auto-scale KafkaConnect Application.

## Project Status

**Project status:** *alpha* 

**Current API version:** *`v1alpha1`*

## Prerequisites

- [kubectl][kubectl_tool] version v1.12.0+.
- Access to a Kubernetes v1.12.0+ cluster.

## Deployment
 - Install the CRDS
```
kubectl apply -f deploy/crds
```
 - Install the operator
```
kubectl apply -f deploy
```

## Getting Started

Get started quickly with the Kubernetes Operator for Apache KafkaConnect using the [User Guide](docs/user-guide.md).

For more information, check the [API Specification](docs/api-docs.md).


## Contributing

Please check [CONTRIBUTING.md](CONTRIBUTING.md) and the [Developer Guide](docs/developer-guide.md) out. 


[kubectl_tool]:https://kubernetes.io/docs/tasks/tools/install-kubectl/


