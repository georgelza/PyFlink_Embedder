FROM arm64v8/flink:1.20.2-scala_2.12-java17
SHELL ["/bin/bash", "-c"]

RUN echo "--> Install some useful tools (openjdk-17-jdk-headless required on ARM platforms to enable installation of apache-flink)" \
    && build_deps="neovim tree lnav unzip gradle python3-pip python3.10-venv openjdk-17-jdk-headless postgresql-client" \
    && apt-get update \
    && apt-get install -y $build_deps

# Set Flink environment variables for Python
ENV PYTHON_HOME=/usr/bin/python3.10
ENV PATH=$PATH:$PYTHON_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

ENV FLINK_HOME=/opt/flink
ENV HIVE_HOME=${FLINK_HOME}/conf/

ENV FLINK_VERSION_SHORT=1.20
ENV FLINK_VERSION_FULL=1.20.2
ENV ICEBERG_VERSION=1.9.1
ENV HADOOP_VERSION=3.3.4
ENV POSTGRESQL_CONNECTOR=42.7.6
ENV FLINK_CDC=3.5.0
ENV PAIMON_VERSION=1.3.1
ENV HADOOP_CLASSPATH=/opt/flink/lib/*

WORKDIR ${FLINK_HOME}

RUN echo "--> Fixing PIP to be able to Handle embedding modules" \
    && python3 -m pip install --upgrade pip setuptools wheel

    
# https://github.com/apache/flink/tree/master/flink-python/pyflink/examples
RUN echo "--> Install apache-flink" \
    && /usr/bin/pip3 install apache-flink==${FLINK_VERSION_FULL}


RUN echo "--> Install pyflink & sentence_transformers (Localized Embedding engine)" \
    && /usr/bin/pip3 install pyflink sentence_transformers


RUN echo "--> Cleanup time" \
    && apt-get remove -y openjdk-17-jdk-headless 


RUN echo "--> Purge apt artifacts" \
    && apt-get purge -y --auto-remove $build_deps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Take note of the below... otherwise your job will report a error saying there is no Python intepreter available.
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ldconfig /usr/lib


RUN echo "--> Setup Directory Structure" && \
    mkdir -p /opt/flink/conf/ && \
    mkdir -p /opt/flink/checkpoints && \
    mkdir -p /opt/flink/rocksdb 


RUN echo "--> Install JARs: Flink's S3 plugin" && \
    mkdir -p ./plugins/s3-fs-hadoop && \
    mv ./opt/flink-s3-fs-hadoop-${FLINK_VERSION_FULL}.jar ./plugins/s3-fs-hadoop/


RUN echo "-> Install JARs: Flink Generic" && \
    mkdir -p ./lib
COPY stage/flink-sql-connector-postgres-cdc-${FLINK_CDC}.jar                    ${FLINK_HOME}/lib/flink-sql-connector-postgres-cdc-${FLINK_CDC}.jar 
COPY stage/postgresql-${POSTGRESQL_CONNECTOR}.jar                               ${FLINK_HOME}/lib/postgresql-${POSTGRESQL_CONNECTOR}.jar 
COPY stage/flink-sql-parquet-${FLINK_VERSION_FULL}.jar                          ${FLINK_HOME}/lib/flink-sql-parquet-${FLINK_VERSION_FULL}.jar
COPY stage/flink-python-${FLINK_VERSION_FULL}.jar                               ${FLINK_HOME}/lib/flink-python-${FLINK_VERSION_FULL}.jar


RUN echo "-> Install JARs: General Hadoop" && \
    mkdir -p ./lib/hadoop 
COPY stage/commons-configuration2-2.1.1.jar                                     ${FLINK_HOME}/lib/commons-configuration2-2.1.1.jar
COPY stage/commons-logging-1.1.3.jar                                            ${FLINK_HOME}/lib/commons-logging-1.1.3.jar 
COPY stage/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar                            ${FLINK_HOME}/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar
COPY stage/hadoop-shaded-guava-1.1.1.jar                                        ${FLINK_HOME}/lib/hadoop-shaded-guava-1.1.1.jar
COPY stage/stax2-api-4.2.1.jar                                                  ${FLINK_HOME}/lib/stax2-api-4.2.1.jar
COPY stage/woodstox-core-5.3.0.jar                                              ${FLINK_HOME}/lib/woodstox-core-5.3.0.jar 
COPY stage/aws-java-sdk-bundle-1.12.262.jar                                     ${FLINK_HOME}/lib/aws-java-sdk-bundle-1.12.262.jar

RUN echo "--> Install JARs: => Paimon" && \
    mkdir -p ./lib/paimon 
COPY stage/paimon-flink-${FLINK_VERSION_SHORT}-${PAIMON_VERSION}.jar            ${FLINK_HOME}/lib/paimon/paimon-flink-${FLINK_VERSION_SHORT}-${PAIMON_VERSION}.jar
COPY stage/paimon-s3-${PAIMON_VERSION}.jar                                      ${FLINK_HOME}/lib/paimon/paimon-s3-${PAIMON_VERSION}.jar

#commented out as we providing the uber jar
#COPY stage/hadoop-auth-${HADOOP_VERSION}.jar                                    ${FLINK_HOME}/lib/hadoop-auth-${HADOOP_VERSION}.jar
#COPY stage/hadoop-common-${HADOOP_VERSION}.jar                                  ${FLINK_HOME}/lib/hadoop-common-${HADOOP_VERSION}.jar
#COPY stage/hadoop-aws-${HADOOP_VERSION}.jar                                     ${FLINK_HOME}/lib/hadoop-aws-${HADOOP_VERSION}.jar 
#COPY stage/hadoop-hdfs-client-${HADOOP_VERSION}.jar                             ${FLINK_HOME}/lib/hadoop-hdfs-client-${HADOOP_VERSION}.jar

# ADD THIS NEW SECTION HERE
### TRYING EVERYTHING TO GET IT WORKING...
RUN echo "-> Create Hadoop Configuration" && \
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


RUN echo "--> Set Ownerships of /opt/flink" && \
    chown -R flink:flink $FLINK_HOME 

USER flink:flink

CMD ./bin/start-cluster.sh && sleep infinity