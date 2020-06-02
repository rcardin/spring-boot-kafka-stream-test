package io.rcardin.spring.kafka.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ApplicationTests {
  
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private TopologyTestDriver testDriver;
  
  @BeforeEach
  void setUp() {
    final StreamsBuilder builder = configureTopology();
    Properties props = buildStreamDummyConfiguration();
    testDriver = new TopologyTestDriver(
        builder.build(),
        Objects.requireNonNull(props)
    );
    inputTopic = testDriver.createInputTopic("words",
        Serdes.String().serializer(),
        Serdes.String().serializer()
    );
    outputTopic = testDriver.createOutputTopic("word-counters",
        Serdes.String().deserializer(),
        Serdes.Long().deserializer()
    );
  }
  
  private StreamsBuilder configureTopology() {
    final Application application = new Application();
    final StreamsBuilder builder = new StreamsBuilder();
    application.kStreamWordCounter(builder);
    return builder;
  }
  
  private Properties buildStreamDummyConfiguration() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.Serdes$StringSerde");
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.Serdes$StringSerde");
    return props;
  }
  
  @AfterEach
  void tearDown() {
    testDriver.close();
  }
  
  @Test
  void contextLoads() {
    inputTopic.pipeInput("key", "Lorem ipsum dolor sit amet, consectetur adipiscing elit "
                                    + "Lorem ipsum dolor sit amet, consectetur adipiscing elit");
    final List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();
    assertThat(results).containsAnyOf(new KeyValue<>("Lorem", 2L));
  }
}
