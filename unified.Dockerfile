FROM debian:11.11-slim AS glowrootbuild
SHELL ["/bin/bash", "-c"]

RUN apt update && apt install -yy curl zip
# Download and extract glowroot
WORKDIR /app/glowroot
RUN curl -s https://api.github.com/repos/glowroot/glowroot/releases/latest \
  | grep browser_download_url | grep "\.zip" | grep -v "central" | cut -d '"' -f 4 \
  | xargs curl -L -o glowroot.zip; unzip glowroot.zip


# upgrade to 3.9 for security updates in due course
FROM maven:3.8.5-eclipse-temurin-17 AS mavenbase
SHELL ["/bin/bash", "-c"]
# For setting up the Maven proxy settings
COPY docker/set_mvn_proxy.sh /app/
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
# We copied the JARs from the previous builds but we need to install them in the local
# maven repo or it won't be able to use them for the main build
#RUN mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange.jar -DgroupId=<group-id> -DartifactId=<artifact-id> -Dversion=<version> -Dpackaging=<packaging>

RUN mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar
RUN mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7-sources.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar -Dclassifier=sources
RUN mvn install:install-file -Dfile=/app/emap-interchange/target/emap-interchange-2.7-tests.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-interchange -Dversion=2.7 -Dpackaging=jar -Dclassifier=tests
RUN mvn install:install-file -Dfile=/app/emap-star/emap-star-annotations/target/emap-star-annotations-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star-annotations -Dversion=2.7 -Dpackaging=jar
RUN mvn install:install-file -Dfile=/app/emap-star/emap-star/target/emap-star-2.7.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star -Dversion=2.7 -Dpackaging=jar
RUN mvn install:install-file -Dfile=/app/emap-star/emap-star/target/emap-star-2.7-sources.jar -DgroupId=uk.ac.ucl.rits.inform -DartifactId=emap-star -Dversion=2.7 -Dpackaging=jar -Dclassifier=sources


COPY core/pom.xml /app/core/
RUN source /app/set_mvn_proxy.sh; mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -f /app/core/pom.xml
COPY core/glowroot.properties glowroot/glowroot.properties
COPY core/config.json glowroot/config.json
COPY core/src/ /app/core/src/
# Create final jar
RUN source /app/set_mvn_proxy.sh; mvn install -Dmaven.test.skip=true -Pemapstar -Dstart-class=uk.ac.ucl.rits.inform.datasinks.emapstar.App

# Runtime base
FROM debian:11.11-slim AS jrebase
ENV JAVA_HOME=/opt/java/openjdk
COPY --from=eclipse-temurin:17.0.14_7-jre $JAVA_HOME $JAVA_HOME
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Core proc runtime
FROM jrebase AS coreruntime
WORKDIR /app/core
COPY --from=corebuild /app /app
COPY --from=glowrootbuild /app/glowroot /app/glowroot
CMD ["java", "-javaagent:/app/glowroot/glowroot/glowroot.jar", "-jar", "./target/core.jar"]
