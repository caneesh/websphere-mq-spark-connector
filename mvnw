#!/bin/sh
# Maven wrapper script

MAVEN_PROJECTBASEDIR="${MAVEN_BASEDIR:-$(cd "$(dirname "$0")" && pwd)}"
MAVEN_WRAPPER_PROPERTIES_FILE="${MAVEN_PROJECTBASEDIR}/.mvn/wrapper/maven-wrapper.properties"
MAVEN_WRAPPER_JAR="${MAVEN_PROJECTBASEDIR}/.mvn/wrapper/maven-wrapper.jar"

MAVEN_VERSION="3.9.6"
MAVEN_DOWNLOAD_URL="https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/${MAVEN_VERSION}/apache-maven-${MAVEN_VERSION}-bin.tar.gz"
MAVEN_HOME="${HOME}/.m2/wrapper/dists/apache-maven-${MAVEN_VERSION}"

if [ ! -d "${MAVEN_HOME}" ]; then
    echo "Downloading Maven ${MAVEN_VERSION}..."
    mkdir -p "${MAVEN_HOME}"
    curl -fsSL "${MAVEN_DOWNLOAD_URL}" | tar -xzf - -C "${MAVEN_HOME}" --strip-components=1
fi

exec "${MAVEN_HOME}/bin/mvn" "$@"
