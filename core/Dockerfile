FROM maven:3.8.5-openjdk-17-slim
# Run commands through bash so source is found
SHELL ["/bin/bash", "-c"]
# Install zip & unzip for glowroot
RUN apt update; apt install -yy zip
# Set up the Maven proxy settings
COPY docker/set_mvn_proxy.sh /app/
# Download and extract glowroot
WORKDIR /app/core
RUN curl -s https://api.github.com/repos/glowroot/glowroot/releases/latest \
  | grep browser_download_url | grep "\.zip" | grep -v "central" | cut -d '"' -f 4 \
  | xargs curl -L -o glowroot.zip; unzip glowroot.zip
COPY core/glowroot.properties glowroot/glowroot.properties
COPY core/config.json glowroot/config.json
# Copy pom and checkstyle, then generate resources (requires emap-star-annotations to be installed)
COPY emap-interchange/pom.xml /app/emap-interchange/
COPY emap-checker.xml /app/
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-interchange/pom.xml
COPY emap-star/pom.xml /app/emap-star/
COPY emap-star/emap-star/pom.xml /app/emap-star/emap-star/
COPY emap-star/emap-star-annotations/ /app/emap-star/emap-star-annotations/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -f /app/emap-star/emap-star-annotations/pom.xml
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-star/pom.xml
COPY core/pom.xml /app/core/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/core/pom.xml
# Set up emap-star
COPY emap-star/emap-star/src/ /app/emap-star/emap-star/src/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -f /app/emap-star/pom.xml
# Set up emap-interchange
COPY emap-interchange/src/ /app/emap-interchange/src/
RUN source /app/set_mvn_proxy.sh; mvn install -f /app/emap-interchange/pom.xml
# Set up core
WORKDIR /app/core
COPY core/src/ /app/core/src/
# Create final jar
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Pemapstar -Dstart-class=uk.ac.ucl.rits.inform.datasinks.emapstar.App
CMD ["java", "-javaagent:./glowroot/glowroot.jar", "-jar", "./target/core.jar"]
