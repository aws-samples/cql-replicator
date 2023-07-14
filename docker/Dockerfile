#Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#SPDX-License-Identifier: Apache-2.0
#CQLReplicator

FROM amazonlinux:latest

ARG AWS_REGION
ENV AWS_REGION=$AWS_REGION

ARG AWS_ACCESS_KEY_ID
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID

ARG AWS_SECRET_ACCESS_KEY
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

ARG AWS_SESSION_TOKEN
ENV AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN

ENV CQLREPLICATOR_WORKING_DIR=/root
WORKDIR $CQLREPLICATOR_WORKING_DIR

RUN yum update -y
RUN rpm --import https://yum.corretto.aws/corretto.key
RUN curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
RUN yum install -y java-17-amazon-corretto-devel
RUN yum install -y awscli unzip
RUN yum clean all

ENV JAVA_OPTS=""
ENV CQLREPLICATOR_NAME_BUCKET_NAME=cqlreplicator

#CQLReplicator SETUP
COPY CQLReplicator.zip $CQLREPLICATOR_WORKING_DIR/
RUN unzip $CQLREPLICATOR_WORKING_DIR/CQLReplicator.zip
RUN chmod 755 $CQLREPLICATOR_WORKING_DIR/CQLReplicator/bin/cqlreplicator.sh
ENV PATH="${PATH}:$CQLREPLICATOR_WORKING_DIR/CQLReplicator/bin"
WORKDIR $CQLREPLICATOR_WORKING_DIR/CQLReplicator/bin
ENTRYPOINT ["cqlreplicator.sh"]
