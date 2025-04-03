FROM debian:11.11-slim AS glowrootbuild
SHELL ["/bin/bash", "-c"]

RUN apt update && apt install -yy curl zip
# Download and extract glowroot
WORKDIR /app/glowroot
RUN curl -s https://api.github.com/repos/glowroot/glowroot/releases/latest \
  | grep browser_download_url | grep "\.zip" | grep -v "central" | cut -d '"' -f 4 \
  | xargs curl -L -o glowroot.zip; unzip glowroot.zip

ARG EMAP_MODULE
COPY $EMAP_MODULE/glowroot.properties /app/glowroot/glowroot/glowroot.properties
COPY $EMAP_MODULE/config.json /app/glowroot/glowroot/config.json


# upgrade to 3.9 for security updates in due course
FROM maven:3.8.5-eclipse-temurin-17 AS mavenbase
SHELL ["/bin/bash", "-c"]
# For setting up the Maven proxy settings
COPY docker/set_mvn_proxy.sh /app/
COPY docker/install_jars_to_mvn.sh /app/
COPY emap-checker.xml /app/

FROM mavenbase AS interchangebuild
# Copy pom and checkstyle, then generate resources (requires emap-star-annotations to be installed)
COPY emap-interchange/pom.xml /app/emap-interchange/
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-interchange/pom.xml
COPY emap-interchange/src/ /app/emap-interchange/src/
RUN source /app/set_mvn_proxy.sh; mvn install -f /app/emap-interchange/pom.xml

FROM mavenbase AS starbuild
COPY emap-star/pom.xml /app/emap-star/
COPY emap-star/emap-star/pom.xml /app/emap-star/emap-star/
COPY emap-star/emap-star-annotations/ /app/emap-star/emap-star-annotations/
# --mount=type=cache,target=/root/.m2,sharing=locked
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -f /app/emap-star/emap-star-annotations/pom.xml
RUN source /app/set_mvn_proxy.sh; mvn dependency:go-offline -f /app/emap-star/pom.xml
COPY emap-star/emap-star/src/ /app/emap-star/emap-star/src/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -f /app/emap-star/pom.xml

# Build core proc
FROM mavenbase AS corebuild
WORKDIR /app/core
# be more selective in what we copy in due course
COPY --from=starbuild /app/emap-star /app/emap-star
COPY --from=interchangebuild /app/emap-interchange /app/emap-interchange
#RUN mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange.jar -DgroupId=<group-id> -DartifactId=<artifact-id> -Dversion=<version> -Dpackaging=<packaging>

RUN bash /app/install_jars_to_mvn.sh emap-interchange
RUN bash /app/install_jars_to_mvn.sh emap-star

COPY core/pom.xml /app/core/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/core/pom.xml
COPY core/src/ /app/core/src/
# Create final jar
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Pemapstar -Dstart-class=uk.ac.ucl.rits.inform.datasinks.emapstar.App

FROM mavenbase AS hl7readerbuild
WORKDIR /app/hl7-reader
# be more selective in what we copy in due course
COPY --from=starbuild /app/emap-star /app/emap-star
COPY --from=interchangebuild /app/emap-interchange /app/emap-interchange
# Install copied JARs from the previous builds so maven can find them
RUN bash /app/install_jars_to_mvn.sh emap-interchange
RUN bash /app/install_jars_to_mvn.sh emap-star

COPY hl7-reader/pom.xml /app/hl7-reader/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/hl7-reader/pom.xml

COPY hl7-reader/src/ /app/hl7-reader/src/
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Dstart-class=uk.ac.ucl.rits.inform.datasources.ids.AppHl7


# Runtime base
FROM debian:11.11-slim AS jrebase
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=eclipse-temurin:17.0.14_7-jre $JAVA_HOME $JAVA_HOME
COPY --from=glowrootbuild /app/glowroot /app/glowroot
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Core proc runtime
FROM jrebase AS coreruntime
COPY --from=corebuild /app/core /app/core
CMD ["java", "-javaagent:/app/glowroot/glowroot/glowroot.jar", "-jar", "/app/core/target/core.jar"]

FROM jrebase AS hl7readerruntime
COPY --from=hl7readerbuild /app/hl7-reader /app/hl7-reader
CMD ["java", "-javaagent:/app/glowroot/glowroot/glowroot.jar", "-jar", "/app/hl7-reader/target/hl7-reader.jar"]
