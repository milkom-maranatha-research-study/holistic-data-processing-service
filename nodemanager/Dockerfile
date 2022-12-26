FROM bde2020/hadoop-base:2.0.0-hadoop3.2.1-java8

# Install netcat to check connection of the dependency services
RUN apt-get update && apt-get install -y netcat

# [Optional] - Uncomment these commands if we want to support python streaming
# RUN apt-get install -y software-properties-common && apt-get update
# RUN apt-get install -y python3-pip python3.5

HEALTHCHECK CMD curl -f http://localhost:8042/ || exit 1

ADD run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 8042

CMD ["/run.sh"]
