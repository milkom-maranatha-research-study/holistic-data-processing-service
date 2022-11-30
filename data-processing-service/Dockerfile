FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Install netcat to check connection of the dependency services
RUN apt-get update && apt-get install -y netcat

# TODO:
# 1. Generate `DataProcessingService.jar` (Including it's Jars' dependencies?)
# 2. Create folder structure to allocate input files in HDFS: `hdfs dfs -mkdir -p /user/root`
# 3. Copy `DataProcessingService.jar` to the `tmp` directory on the `namenode` container
#    docker cp data-processing-service/DataProcessingService.jar namenode:/tmp
# 4. Copy input files to the `tmp` directory on the `namenode` container
#    docker cp data-processing-service/input/* namenode:/tmp/input
# 5. Copy input files from the `tmp` directory to the HDFS /user/root/input directory
#    hdfs dfs -mkdir /user/root/input
#    hdfs dfs -put tmp/input/* /user/root/input/*
# 6. Create HDFS DFS Input Directory and its files

# COPY DataProcessingService.jar /opt/hadoop/applications/DataProcessingService.jar

# ENV JAR_FILEPATH="/opt/hadoop/applications/DataProcessingService.jar"
# ENV CLASS_TO_RUN="DataProcessingService"
# ENV PARAMS="/input /output"

ADD run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
