FROM debezium/connect:latest

COPY ./debezium-oracle-connector.properties /kafka/connectors/
# Add Oracle JDBC driver
ADD ./ojdbc8.jar /kafka/libs/ojdbc8.jar