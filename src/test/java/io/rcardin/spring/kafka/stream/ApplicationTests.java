package io.rcardin.spring.kafka.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
class ApplicationTests {
  
  @Autowired
  private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private TopologyTestDriver testDriver;
  
  @BeforeEach
  void setUp() {
    testDriver = new TopologyTestDriver(
        streamsBuilderFactoryBean.getTopology(),
        Objects.requireNonNull(streamsBuilderFactoryBean.getStreamsConfiguration())
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
  
  @AfterEach
  void tearDown() {
    testDriver.close();
    streamsBuilderFactoryBean.getKafkaStreams().close();
  }
  
  @Test
  void contextLoads() {
    inputTopic.pipeInput("key", "Lorem ipsum dolor sit amet, consectetur adipiscing elit "
                                    + "Lorem ipsum dolor sit amet, consectetur adipiscing elit");
    final List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();
    assertThat(results).containsAnyOf(new KeyValue<>("Lorem", 2L));
  }
}
