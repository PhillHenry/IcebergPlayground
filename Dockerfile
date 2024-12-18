# run the shadowDistZip Gradle task then
# cp POLARIS_SRC_HOME/polaris-service/build/distributions/polaris-service-shadow-999-SNAPSHOT.zip .
# Build with something like:
# docker build --build-arg UBER_JAR=polaris-service-shadow-999-SNAPSHOT  -t my-polaris .
# docker tag $(docker images | grep my-polaris  | awk '{print $3}') ph1ll1phenry/polaris_for_bdd
# docker push ph1ll1phenry/polaris_for_bdd

FROM ubuntu:24.10

RUN apt update
RUN apt install -y openjdk-21-jdk
RUN apt install -y unzip
RUN apt-get install lsof

# Set the working directory in the container, nuke any existing builds
WORKDIR /app
RUN rm -rf build

ARG UBER_JAR
ARG TARGET=/app

WORKDIR /app
COPY ${UBER_JAR}.zip ${TARGET}
RUN unzip /app/${UBER_JAR}.zip
RUN mv ${TARGET}/${UBER_JAR}/* ${TARGET}
COPY polaris-server.yml ${TARGET}
COPY polaris_entrypoint.sh ${TARGET}
RUN chmod a+rx ${TARGET}/polaris_entrypoint.sh
RUN chmod -R a+rwx ${TARGET}

EXPOSE 8181

# Run the resulting java binary
ENTRYPOINT ["/bin/bash"]
CMD ["/app/polaris_entrypoint.sh"]
