FROM jfrog-internal.sensorsdata.cn/docker-develop/com.sensorsdata.dragon/bite:0.5.0.3081-el9_aarch64
WORKDIR /data/code

RUN yum install -y netcat \
    && yum install -y gflags-devel \
    && yum install -y zlib-devel

RUN dnf --enablerepo=crb install snappy-devel -y

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0
ENV LD_LIBRARY_PATH=/usr/lib/jvm/java-1.8.0/jre/lib/aarch64/server
