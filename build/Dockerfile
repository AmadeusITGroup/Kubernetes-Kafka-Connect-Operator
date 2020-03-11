FROM quay.io/slok/kube-code-generator:v1.15.10 as builder
WORKDIR /go/src/github.com/amadeusitgroup/kubernetes-kafka-connect-operator

COPY --chown=codegen:codegen . .
ENV CODEGEN_PKG /go/src/k8s.io/code-generator
# Re-generate generated code
RUN hack/update-codegen.sh
USER root
ENV GOOS=linux \
    CGO_ENABLED=0
RUN go build -o build/_output/bin/Kubernetes-Kafka-Connect-Operator \
    -gcflags all=-trimpath=/go/src/github.com/AmadeusITGroup \
    -asmflags all=-trimpath=/go/src/github.com/AmadeusITGroup \
    github.com/amadeusitgroup/kubernetes-kafka-connect-operator/cmd/manager

FROM alpine
ENV OPERATOR=/usr/local/bin/Kubernetes-Kafka-Connect-Operator \
    USER_UID=1001 \
    USER_NAME=Kubernetes-Kafka-Connect-Operator

COPY --from=builder /go/src/github.com/amadeusitgroup/kubernetes-kafka-connect-operator/build/_output/bin/Kubernetes-Kafka-Connect-Operator ${OPERATOR}
COPY build/bin /usr/local/bin

RUN  /usr/local/bin/user_setup
ENTRYPOINT ["/usr/local/bin/entrypoint"]
USER ${USER_UID}