package br.com.alura.ecommerce;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaService<T> implements Closeable {

  private final KafkaConsumer<String, T> consumer;
  private final ConsumerFunction parse;

  private KafkaService(ConsumerFunction parse, String groupId, Class<T> type, Map<String, String> properties) {
    this.parse = parse;
    this.consumer = new KafkaConsumer<>(
        getPproperties(type, groupId, properties));
  }

  KafkaService(String groupId, String topic, ConsumerFunction parse, Class<T> type, Map<String, String> properties) {
    this(parse, groupId, type, properties);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public KafkaService(String groupId, Pattern topic, ConsumerFunction parse, Class<T> type,
      Map<String, String> properties) {
    this(parse, groupId, type, properties);
    consumer.subscribe(topic);
  }

  void run() {
    while (true) {
      var records = consumer.poll(Duration.ofMillis(100));

      if (!records.isEmpty()) {
        System.out.println("Encontrei " + records.count() + " registros");
        for (var record : records) {
          parse.consume(record);
        }
      }
    }
  }

  private Properties getPproperties(Class<T> type, String groupId, Map<String, String> overridePproperties) {
    var properties = new Properties();

    // qual o endereço do servidor
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.37:9092");

    // deserializar o valor e a chave recebida de binario para string
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

    // quando criamos um consumer precisamos dizer qual é o id do grupo
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

    properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
    properties.putAll(overridePproperties);
    return properties;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
