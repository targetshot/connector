FROM confluentinc/cp-kafka-connect:7.7.0
ENV DEBEZIUM_VERSION=3.2.3.Final
RUN curl -fsSL https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz \
  | tar -xz -C /usr/share/java
RUN curl -fsSL https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/3.5.6/mariadb-java-client-3.5.6.jar \
  -o /usr/share/java/mariadb-java-client.jar
