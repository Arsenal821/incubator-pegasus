#FROM jfrog-internal.sensorsdata.cn/docker-test/com.sensorsdata.armada/el9_base_java8:0.1.6.7

FROM jfrog-internal.sensorsdata.cn/docker-test/com.sensorsdata.armada/el7_base_java8:0.3.0

USER sa_cluster
ARG MODULE_NAME=skv_offline
 
ENV SENSORS_SKV_HOME=/sensorsdata/main/program/skv
ENV SENSORS_SKV_LOG_DIR=/sensorsdata/main/logs/skv
ENV SENSORS_SKV_RUNTIME_DIR=/sensorsdata/main/runtime/skv
ENV SKV_HOME=/sensorsdata/main/program/skv
RUN mkdir -p ${SENSORS_SKV_HOME}/${MODULE_NAME} ${SENSORS_SKV_HOME}/${MODULE_NAME}/${MODULE_NAME} ${SENSORS_SKV_LOG_DIR}/${MODULE_NAME} ${SENSORS_SKV_RUNTIME_DIR}/${MODULE_NAME}

# 注意目录结构要和正常安装环境保持一致
COPY --chown=sa_cluster:sa_group ./skv_binary/ $SENSORS_SKV_HOME/${MODULE_NAME}/${MODULE_NAME}
COPY --chown=sa_cluster:sa_group ./admintools/ ${SENSORS_SKV_HOME}/admintools
COPY --chown=sa_cluster:sa_group ./skv_shim/ ${SENSORS_SKV_HOME}/skv_shim

## 建立软链，适配宿主机的代码
RUN ln -s ${SENSORS_SKV_HOME}/${MODULE_NAME}/${MODULE_NAME} ${SENSORS_SKV_HOME}/${MODULE_NAME}/replica_server && \
    ln -s ${SENSORS_SKV_HOME}/${MODULE_NAME}/${MODULE_NAME} ${SENSORS_SKV_HOME}/${MODULE_NAME}/meta_server

## 安装hyperion
RUN /opt/install_hyperion.sh

