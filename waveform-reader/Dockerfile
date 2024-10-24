FROM maven:3.8.5-openjdk-17-slim AS emap_java_base
# Run commands through bash so source is found
SHELL ["/bin/bash", "-c"]
# Install zip & unzip for glowroot
RUN apt update; apt install -yy zip
# Set up the Maven proxy settings
COPY docker/set_mvn_proxy.sh /app/
# Download and extract glowroot
ARG SERVICE_SRC_DIR
WORKDIR /app/$SERVICE_SRC_DIR
RUN curl -s https://api.github.com/repos/glowroot/glowroot/releases/latest \
  | grep browser_download_url | grep "\.zip" | grep -v "central" | cut -d '"' -f 4 \
  | xargs curl -L -o glowroot.zip; unzip glowroot.zip
COPY $SERVICE_SRC_DIR/glowroot.properties glowroot/glowroot.properties
COPY $SERVICE_SRC_DIR/config.json glowroot/config.json
# Copy pom and checkstyle, then all generate resources
COPY emap-checker.xml /app/
COPY emap-interchange/pom.xml /app/emap-interchange/
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-interchange/pom.xml
COPY $SERVICE_SRC_DIR/pom.xml /app/$SERVICE_SRC_DIR/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/$SERVICE_SRC_DIR/pom.xml
# Install emap-interchange
COPY emap-interchange/src/ /app/emap-interchange/src/
RUN source /app/set_mvn_proxy.sh; mvn install -f /app/emap-interchange/pom.xml
# Install waveform-reader
WORKDIR /app/$SERVICE_SRC_DIR
COPY $SERVICE_SRC_DIR/src/ /app/$SERVICE_SRC_DIR/src/

FROM emap_java_base AS waveform_reader
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Dstart-class=uk.ac.ucl.rits.inform.datasources.waveform.Application
CMD ["java",  "-javaagent:./glowroot/glowroot.jar", "-jar", "./target/waveform-reader.jar"]

FROM emap_java_base AS waveform_generator
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Dstart-class=uk.ac.ucl.rits.inform.datasources.waveform_generator.Application
CMD ["java",  "-javaagent:./glowroot/glowroot.jar", "-jar", "./target/waveform-generator.jar"]

