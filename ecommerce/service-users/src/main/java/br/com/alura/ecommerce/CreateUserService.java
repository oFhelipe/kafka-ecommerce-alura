package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class CreateUserService {

  private Connection connection;

  CreateUserService() throws SQLException {
    String url = "jdbc:sqlite:ecommerce/service-users/target/users_database.db";
    this.connection = DriverManager.getConnection(url);
    try {
      connection.createStatement().execute(
          "CREATE TABLE Users (" +
              "uuid varchar(200) primary key," +
              "email varchar(200)" +
              ")");
    } catch (SQLException ex) {
      // Cuidado, o script sql pode estar errado
      ex.printStackTrace();
    }
  }

  public static void main(String[] args) throws SQLException {
    var createUserService = new CreateUserService();
    try (var service = new KafkaService<>(CreateUserService.class.getSimpleName(),
        "ECOMMERCE_NEW_ORDER",
        createUserService::parse,
        Order.class,
        Map.of())) {
      service.run();
    }

  }

  private void parse(ConsumerRecord<String, Order> record) throws SQLException {
    System.out.println("------------------------------------------");
    System.out.println("Processing new order, checking for new user");
    System.out.println("value: " + record.value());
    var order = record.value();
    if (isNewUser(order.getEmail())) {
      insertNewUser(order.getEmail());
    }
  }

  private void insertNewUser(String email) throws SQLException {
    var insert = connection.prepareStatement("INSERT INTO Users (uuid, email) " +
        "VALUES (?, ?)");
    var id = UUID.randomUUID().toString();
    insert.setString(1, id);
    insert.setString(2, email);
    insert.execute();

    System.out.println("Usuario com id " + id + " e email " + email + " adicionado");
  }

  private boolean isNewUser(String email) throws SQLException {
    var exists = connection.prepareStatement("SELECT uuid from Users WHERE email = ? LIMIT 1");
    exists.setString(1, email);
    var results = exists.executeQuery();
    return !results.next();
  }
}