package io.rcardin.spring.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Arrays;

@EnableKafkaStreams
@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public KStream<String, Long> kStreamWordCounter(StreamsBuilder streamsBuilder) {
		final KStream<String, Long> wordCountStream = streamsBuilder
				.stream("words", Consumed.with(Serdes.String(), Serdes.String()))
				.flatMapValues(word -> Arrays.asList(word.split(" ")))
				.map(((key, value) -> new KeyValue<>(value, value)))
				.groupByKey()
				.count()
				.toStream();
		wordCountStream.to("word-counters", Produced.with(Serdes.String(), Serdes.Long()));
		return wordCountStream;
	}
}
