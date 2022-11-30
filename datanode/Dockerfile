FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Install netcat to check connection of the dependency services
RUN apt-get update && apt-get install -y netcat

# [Optional] - Only relevant if we want to support python streaming
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3

HEALTHCHECK CMD curl -f http://localhost:9864/ || exit 1

ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data
VOLUME /hadoop/dfs/data

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9864

CMD ["/run.sh"]
