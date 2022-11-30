FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Install netcat to check connection of the dependency services
RUN apt-get update && apt-get install -y netcat

# [Optional] - Only relevant if we want to support python streaming
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y python3

HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870

CMD ["/run.sh"]
