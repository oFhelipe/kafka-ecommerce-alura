package br.com.alura.ecommerce;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaDispatcher<T> implements Closeable {
  private KafkaProducer<String, T> producer;

  KafkaDispatcher() {
    this.producer = new KafkaProducer<>(properties());
  }

  // iniciando e configurando as propriedades da conexão
  private static Properties properties() {
    var properties = new Properties();
    // adicionando o endereco
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.100.37:9092");

    // Para serializar string em bytes
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());

    return properties;
  }

  void send(String topic, String key, T value) throws InterruptedException, ExecutionException {

    // Criando um registro a ser enviado
    var record = new ProducerRecord<>(topic, key, value);

    Callback callback = (data, ex) -> {
      if (ex != null) {
        ex.printStackTrace();
        return;
      }
      System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset "
          + data.offset() + "/ timestamp " + data.timestamp());
    };

    // enviando registro com o producer
    producer.send(record, callback).get();
  }

  @Override
  public void close(){
    producer.close();
  }
}
