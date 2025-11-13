FROM maven:3.8.5-openjdk-17-slim
# Run commands through bash so source is found
SHELL ["/bin/bash", "-c"]
# Install zip & unzip for glowroot
RUN apt update; apt install -yy zip
# Set up the Maven proxy settings
COPY docker/set_mvn_proxy.sh /app/
# Download and extract glowroot
WORKDIR /app/hl7-reader
RUN curl -s https://api.github.com/repos/glowroot/glowroot/releases/latest \
  | grep browser_download_url | grep "\.zip" | grep -v "central" | cut -d '"' -f 4 \
  | xargs curl -L -o glowroot.zip; unzip glowroot.zip
COPY hl7-reader/glowroot.properties glowroot/glowroot.properties
COPY hl7-reader/config.json glowroot/config.json
# Copy pom and checkstyle, then all generate resources
COPY emap-checker.xml /app/
COPY emap-interchange/pom.xml /app/emap-interchange/
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-interchange/pom.xml
COPY hl7-reader/pom.xml /app/hl7-reader/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/hl7-reader/pom.xml
# Install emap-interchange
COPY emap-interchange/src/ /app/emap-interchange/src/
RUN source /app/set_mvn_proxy.sh; mvn install -f /app/emap-interchange/pom.xml

COPY emap-star/ /app/emap-star/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -f /app/emap-star/pom.xml

# Install hl7-reader
WORKDIR /app/hl7-reader
COPY hl7-reader/src/ /app/hl7-reader/src/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Dstart-class=uk.ac.ucl.rits.inform.datasources.ids.AppHl7
CMD ["java",  "-javaagent:./glowroot/glowroot.jar", "-jar", "./target/hl7-reader.jar"]
