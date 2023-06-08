FROM jfrog-internal.sensorsdata.cn/docker-develop/com.sensorsdata.dragon/bite:0.5.0.3081-el7_x86_64
WORKDIR /data/code

ENV SKV_PY_BUILD_TOOLS \
    centos-release-scl \
    scl-utils \
    epel-release \
    autoconf \
    libtool \
    file \
    wget \
    unzip \
    which \
    libaio-devel \
    snappy-devel \
    lz4-devel \
    bison \
    flex \
    patch \
    gcc-c++ \
    poco-devel \
    openssl-devel \
    gflags-devel \
    bzip2 \
    krb5-devel \
    cyrus-sasl-devel \
    boost-devel

RUN yum install -y ${SKV_PY_BUILD_TOOLS} \
    && yum install -y libzstd-devel \
    && yum -y remove gcc g++ \
    && yum -y install devtoolset-7-gcc \
    && yum -y install devtoolset-7-gcc-c++ \
    && yum install -y java \
    && yum install -y netcat

RUN wget --progress=dot:giga https://archive.apache.org/dist/maven/maven-3/3.8.3/binaries/apache-maven-3.8.3-bin.tar.gz -P /opt/maven \
    && cd /opt/maven \
    && tar -zxf apache-maven-3.8.3-bin.tar.gz \
    && rm apache-maven-3.8.3-bin.tar.gz

RUN python3 -m pip install --upgrade pip \
    && pip3 install --no-cache-dir cmake==3.16.6

ENV PATH="/opt/rh/devtoolset-7/root/bin/:${PATH}"
ENV PATH="/opt/maven/apache-maven-3.8.3/bin:${PATH}"
