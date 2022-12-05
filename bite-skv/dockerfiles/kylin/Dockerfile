FROM jfrog-internal.sensorsdata.cn/docker-test/com.sensorsdata.armada/el9_base_dev:0.1.6.2
WORKDIR /data/code

RUN yum install -y netcat \
    && yum install -y gflags-devel

RUN dnf --enablerepo=crb install snappy-devel -y

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0
ENV LD_LIBRARY_PATH=/usr/lib/jvm/java-1.8.0/jre/lib/aarch64/server
