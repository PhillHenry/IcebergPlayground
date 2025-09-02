# run the shadowDistZip Gradle task then
# cp POLARIS_SRC_HOME/polaris-service/build/distributions/polaris-service-shadow-999-SNAPSHOT.zip .
# Build with something like:
# docker build --build-arg UBER_JAR=polaris-service-shadow-999-SNAPSHOT  -t my-polaris .
# docker tag $(docker images | grep my-polaris  | awk '{print $3}') ph1ll1phenry/polaris_for_bdd
# docker push ph1ll1phenry/polaris_for_bdd

FROM ubuntu:24.10

# Install everything in one layer: bash, java, unzip, lsof
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk unzip lsof bash coreutils && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app
RUN rm -rf build

# Arguments for build
ARG UBER_JAR
ARG TARGET=/app

# Copy and unpack your jar
COPY ${UBER_JAR}.zip ${TARGET}
RUN unzip ${TARGET}/${UBER_JAR}.zip -d ${TARGET} && \
    mv ${TARGET}/${UBER_JAR}/* ${TARGET}
COPY ./target/quarkus-app/quarkus/quarkus-application.dat ${TARGET}/server/quarkus

# Copy config and entrypoint
COPY polaris-server.yml ${TARGET}/server
COPY polaris_entrypoint.sh ${TARGET}
RUN chmod a+rx ${TARGET}/polaris_entrypoint.sh && chmod -R a+rwx ${TARGET}

# Expose port
EXPOSE 8181

# Run entrypoint
ENTRYPOINT ["/app/polaris_entrypoint.sh"]

