FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

ENV OPERATOR=/usr/local/bin/Kubernetes-Kafka-Connect-Operator \
    USER_UID=1001 \
    USER_NAME=Kubernetes-Kafka-Connect-Operator

# install operator binary
COPY build/_output/bin/Kubernetes-Kafka-Connect-Operator ${OPERATOR}

COPY build/bin /usr/local/bin
RUN  /usr/local/bin/user_setup

ENTRYPOINT ["/usr/local/bin/entrypoint"]

USER ${USER_UID}
