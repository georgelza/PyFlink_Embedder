FROM arm64v8/flink:1.20.2-scala_2.12-java17
SHELL ["/bin/bash", "-c"]

# 1. Install necessary system tools 
RUN echo "--> Installing system dependencies" \
    && build_deps="neovim tree lnav unzip gradle python3-pip python3.10-venv openjdk-17-jdk-headless postgresql-client" \
    && apt-get update \
    && apt-get install -y $build_deps

# 2. Environment Variables [cite: 1, 2]
ENV PYTHON_HOME=/usr/bin/python3.10
ENV PATH=$PATH:$PYTHON_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV FLINK_HOME=/opt/flink
ENV HIVE_HOME=${FLINK_HOME}/conf/
ENV FLINK_VERSION_SHORT=1.20
ENV FLINK_VERSION_FULL=1.20.2
ENV PAIMON_VERSION=1.3.1
ENV POSTGRESQL_CONNECTOR=42.7.6
ENV FLINK_CDC=3.5.0

# FIX: Classpath must include all directories containing your JARs [cite: 1, 8]
ENV HADOOP_CLASSPATH=/opt/flink/lib/*

WORKDIR ${FLINK_HOME}

# 3. Python Setup [cite: 1, 2, 3]
RUN echo "--> Setup Python Environment" \
    && python3 -m pip install --upgrade pip setuptools wheel \
    && /usr/bin/pip3 install apache-flink==${FLINK_VERSION_FULL} \
    && /usr/bin/pip3 install pyflink sentence_transformers \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && ldconfig /usr/lib

# 4. Directory Structure 
RUN mkdir -p /opt/flink/conf/ /opt/flink/checkpoints /opt/flink/rocksdb /opt/flink/lib

# 5. Install JARs: S3 Plugin (Internal Flink System) 
RUN mkdir -p ./plugins/s3-fs-hadoop && \
    mv /opt/flink/opt/flink-s3-fs-hadoop-${FLINK_VERSION_FULL}.jar ./plugins/s3-fs-hadoop/

# 6. Install JARs: Connectors & Hadoop [cite: 4, 5, 6, 7, 8]
# Note: Moving all JARs to /lib/ ensures they are in the default system classpath
COPY stage/flink-sql-connector-postgres-cdc-${FLINK_CDC}.jar    ${FLINK_HOME}/lib/
COPY stage/postgresql-${POSTGRESQL_CONNECTOR}.jar              ${FLINK_HOME}/lib/
COPY stage/flink-sql-parquet-${FLINK_VERSION_FULL}.jar         ${FLINK_HOME}/lib/
COPY stage/flink-python-${FLINK_VERSION_FULL}.jar              ${FLINK_HOME}/lib/
COPY stage/commons-configuration2-2.1.1.jar                    ${FLINK_HOME}/lib/
COPY stage/commons-logging-1.1.3.jar                           ${FLINK_HOME}/lib/
COPY stage/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar           ${FLINK_HOME}/lib/
COPY stage/hadoop-shaded-guava-1.1.1.jar                       ${FLINK_HOME}/lib/
COPY stage/stax2-api-4.2.1.jar                                 ${FLINK_HOME}/lib/
COPY stage/woodstox-core-5.3.0.jar                             ${FLINK_HOME}/lib/
COPY stage/aws-java-sdk-bundle-1.12.262.jar                    ${FLINK_HOME}/lib/

# Paimon JARs 
COPY stage/paimon-flink-${FLINK_VERSION_SHORT}-${PAIMON_VERSION}.jar ${FLINK_HOME}/lib/
COPY stage/paimon-s3-${PAIMON_VERSION}.jar                           ${FLINK_HOME}/lib/

# 7. Hadoop Configuration for S3A (MinIO) [cite: 10, 11, 12]
RUN echo "--> Create Hadoop Configuration" && \
    cat > ${HIVE_HOME}/core-site.xml <<'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>mnadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>mnpassword</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
</configuration>
EOF

# 8. Cleanup and Permissions [cite: 12]
RUN apt-get clean && rm -rf /var/lib/apt/lists/* && \
    chown -R flink:flink $FLINK_HOME 

USER flink:flink
CMD ./bin/start-cluster.sh && sleep infinity