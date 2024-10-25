FROM ubuntu:24.10

RUN apt update
RUN apt install -y openjdk-21-jdk
RUN apt install -y unzip

# Copy the REST catalog into the container
#COPY --chown=default:root . /app

RUN useradd -ms /bin/bash henryp
USER henryp

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

EXPOSE 8181

# Run the resulting java binary
ENTRYPOINT ["/bin/bash"]
CMD ["/app/bin/polaris-service", "server", "polaris-server.yml"]
