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

RUN echo "--> Fixing PIP to be able to Handle embedding modules" \
    && python3 -m pip install --upgrade pip setuptools wheel

# https://github.com/apache/flink/tree/master/flink-python/pyflink/examples
RUN echo "--> Install apache-flink" \
    && /usr/bin/pip3 install apache-flink==1.20.2

RUN echo "--> Install pyflink & sentence_transformers" \
    && /usr/bin/pip3 install pyflink sentence_transformers

RUN echo "--> Cleanup time" \
    && apt-get remove -y openjdk-17-jdk-headless 

RUN echo "--> Purge apt artifacts" \
    && apt-get purge -y --auto-remove $build_deps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# # Take note of the below... otherwise your job will report a error saying there is no Python intepreter available.
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ldconfig /usr/lib

ENV FLINK_HOME=/opt/flink
ENV HIVE_HOME=$FLINK_HOME/conf/
WORKDIR $FLINK_HOME


RUN echo "--> Setup Directory Structure" && \
    mkdir -p /opt/flink/conf/ && \
    mkdir -p /opt/flink/checkpoints && \
    mkdir -p /opt/flink/rocksdb 

RUN echo "--> Install JARs: Flink's S3 plugin" && \
    mkdir -p ./plugins/s3-fs-hadoop && \
    mv ./opt/flink-s3-fs-hadoop-1.20.2.jar ./plugins/s3-fs-hadoop/

RUN echo "--> Install Flink JARs: Generic"
COPY stage/bundle-2.31.9.jar                            /opt/flink/lib/bundle-2.31.9.jar  
COPY stage/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar    /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

COPY stage/postgresql-42.7.6.jar                        /opt/flink/lib/postgresql-42.7.6.jar
# CDC Support https://nightlies.apache.org/flink/flink-cdc-docs-release-3.4/docs/connectors/flink-sources/overview/#supported-flink-versions
COPY stage/flink-sql-connector-postgres-cdc-3.5.0.jar   /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar
COPY stage/flink-sql-json-1.20.2.jar                    /opt/flink/lib/flink-sql-json-1.20.2.jar     
COPY stage/flink-sql-parquet-1.20.2.jar                 /opt/flink/lib/flink-sql-parquet-1.20.2.jar
COPY stage/flink-json-1.20.2.jar                        /opt/flink/lib/flink-json-1.20.2.jar
COPY stage/flink-connector-jdbc-3.3.0-1.20.jar          /opt/flink/lib/flink-connector-jdbc-3.3.0-1.20.jar

COPY stage/paimon-flink-1.20-1.2.0.jar                  /opt/flink/lib/paimon-flink-1.20-1.2.0.jar
COPY stage/paimon-s3-1.3.1.jar                          /opt/flink/lib/paimon-s3-1.3.1.jar 

RUN echo "--> Install Fluss JARs into Flink Lib Folder"
COPY stage/fluss-flink-1.20-0.8.0-incubating.jar        /opt/flink/lib/fluss-flink-1.20-0.8.0-incubating.jar
COPY stage/fluss-flink-tiering-0.8.0-incubating.jar     /opt/flink/lib/fluss-flink-tiering-0.8.0-incubating.jar
COPY stage/fluss-fs-s3-0.8.0-incubating.jar             /opt/flink/lib/fluss-fs-s3-0.8.0-incubating.jar
COPY stage/fluss-lake-paimon-0.8.0-incubating.jar       /opt/flink/lib/fluss-lake-paimon-0.8.0-incubating.jar
COPY stage/fluss-lake-iceberg-0.8.0-incubating.jar      /opt/flink/lib/fluss-lake-iceberg-0.8.0-incubating.jar
COPY stage/iceberg-flink-1.20-1.9.1.jar                 /opt/flink/lib/iceberg-flink-1.20-1.9.1.jar

COPY stage/iceberg-flink-runtime-1.20-1.9.1.jar         /opt/flink/lib/iceberg-flink-runtime-1.20-1.9.1.jar
COPY stage/iceberg-aws-bundle-1.9.1.jar                 /opt/flink/iceberg-aws-bundle-1.9.1.jar

# COPY stage/fluss-lake-lance-0.8.0-incubating.jar        /opt/flink/lib/fluss-lake-lance-0.8.0-incubating.jar


RUN echo "--> Set Ownerships of /opt/flink" && \
    chown -R flink:flink $FLINK_HOME 

USER flink:flink

CMD ./bin/start-cluster.sh && sleep infinity
