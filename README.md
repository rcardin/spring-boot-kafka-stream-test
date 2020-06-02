# Testing Kafka Streams using Spring Boot
Toy project to show how to test kafka streams application using Spring Boot.

In details, Spring Boot manages the creation of Kafka Streams. The library 
[kafka-streams-test-utils](https://kafka.apache.org/documentation/streams/developer-guide/testing.html) 
manages the interaction with the broker.

The good part is that no broker starts during tests, not even an embedded Kafka :) 
Moreover, the test is a pure JUnit test and requires no Spring context configuration.
