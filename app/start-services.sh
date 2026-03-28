#!/bin/bash

set -e

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

echo "================================================================"
echo " start-services.sh"
echo "================================================================"

echo "[INFO] Writing yarn-site.xml ..."
cat > "$HADOOP_CONF_DIR/yarn-site.xml" << 'YARN_EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>cluster-master</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>cluster-master:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>cluster-master:8032</value>
  </property>
  <property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>cluster-master:8088</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>cluster-master:8031</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>cluster-master:8033</value>
  </property>
  <property>
    <name>yarn.nodemanager.disk-health-checker.enable</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.resourcemanager.proxy-user-privileges.enabled</name>
    <value>true</value>
  </property>
  <!--
    Use port 8046 for the NodeManager resource-localizer service.
    The default port 8040 is sometimes still bound by a previous NodeManager
    process that did not exit cleanly, causing the new NodeManager to fail
    with "Address already in use".
  -->
  <property>
    <name>yarn.nodemanager.localizer.address</name>
    <value>0.0.0.0:8046</value>
  </property>
</configuration>
YARN_EOF
echo "[INFO] yarn-site.xml written."

echo "[INFO] Writing spark-defaults.conf ..."
cat > /usr/local/spark/conf/spark-defaults.conf << 'SPARK_EOF'
spark.yarn.archive=hdfs:///apps/spark/spark-jars.zip
SPARK_EOF
echo "[INFO] spark-defaults.conf written."

echo "localhost" > "$HADOOP_CONF_DIR/workers"
echo "[INFO] Hadoop workers set to: localhost"

NAMENODE_DIR="$HADOOP_HOME/hdfs/namenode"
if [ ! -d "$NAMENODE_DIR/current" ]; then
    echo "[INFO] Namenode not formatted — formatting now ..."
    hdfs namenode -format -force -nonInteractive
    echo "[INFO] Namenode formatted."
else
    echo "[INFO] Namenode already formatted — skipping format."
fi

echo "[INFO] Starting HDFS daemons ..."
"$HADOOP_HOME/sbin/start-dfs.sh"

echo "[INFO] Starting YARN daemons ..."
"$HADOOP_HOME/sbin/start-yarn.sh"

echo "[INFO] Starting MapReduce History Server ..."
mapred --daemon start historyserver

echo "[INFO] Waiting 5 s for daemons to stabilise ..."
sleep 5

echo "[INFO] Active JVM processes:"
jps -lm

echo "[INFO] HDFS report:"
hdfs dfsadmin -report

echo "[INFO] Leaving safemode (if active) ..."
hdfs dfsadmin -safemode leave

echo "[INFO] Creating HDFS directories ..."
hdfs dfs -mkdir -p /user/root
hdfs dfs -mkdir -p /apps/spark
echo "[INFO] HDFS directories ready."

if hdfs dfs -test -e /apps/spark/spark-jars.zip 2>/dev/null; then
    echo "[INFO] /apps/spark/spark-jars.zip already in HDFS — skipping upload."
else
    echo "[INFO] Packaging Spark JARs into a single archive (~300 MB) ..."
    cd /usr/local/spark/jars
    zip -q /tmp/spark-jars.zip ./*.jar
    echo "[INFO] Uploading spark-jars.zip to HDFS ..."
    hdfs dfs -put /tmp/spark-jars.zip /apps/spark/spark-jars.zip
    rm -f /tmp/spark-jars.zip
    echo "[INFO] spark-jars.zip uploaded to HDFS."
fi

echo ""
echo "================================================================"
echo " start-services.sh complete."
echo "================================================================"
jps -lm
