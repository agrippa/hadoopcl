export JAVA_HOME=/opt/apps/jdk/1.6
export HOME=/home/jmg3
export HADOOP_HOME=${HOME}/hadoop-1.0.3
export APARAPI_HOME=${HOME}/aparapi-read-only
export HADOOP_OPTS=-Djava.net.preferIPv4Stack=true
export HADOOPCL_KERNEL_FOLDER=${HOME}/kernels
export HADOOP_LOG_DIR=/tmp/hadoop/logs
export HADOOP_APP_DIR=${HOME}/app
export CLASSPATH=${APARAPI_HOME}/com.amd.aparapi/dist/aparapi.jar:${HADOOP_HOME}/build/hadoop-core-1.0.4-SNAPSHOT.jar:${HOME}/hadoop-gpl-compression-read-only/build/hadoop-gpl-compression-0.2.0-dev.jar:${HADOOP_HOME}/build/hadoop-core-1.0.4-SNAPSHOT.jar:${HOME}/jfreechart-1.0.14.jar:.:${CLASSPATH}:/home/jmg3/mahout/math/target/mahout-math-0.9-SNAPSHOT.jar:/home/jmg3/mahout/integration/target/dependency/mahout-core-0.9-SNAPSHOT.jar
export HADOOP_CLASSPATH=${HOME}/hadoop-gpl-compression-read-only/build/hadoop-gpl-compression-0.2.0-dev.jar:${CLASSPATH}:${HADOOP_CLASSPATH}:/home/jmg3/mahout/math/target/mahout-math-0.9-SNAPSHOT.jar:/home/jmg3/mahout/integration/target/dependency/mahout-core-0.9-SNAPSHOT.jar
export JAVA_LIBRARY_PATH=${HOME}/lzo-install/lib:${JAVA_LIBRARY_PATH}

export SCIPY_HOME=${HOME}/scipy-install
export NUMPY_HOME=${HOME}/numpy-install
export SCIKIT_HOME=${HOME}/scikit-install
export PYTHONPATH=${SCIKIT_HOME}/lib64/python2.6/site-packages:${NUMPY_HOME}/lib64/python2.6/site-packages:${SCIPY_HOME}/lib64/python2.6/site-packages:${PYTHONPATH}

