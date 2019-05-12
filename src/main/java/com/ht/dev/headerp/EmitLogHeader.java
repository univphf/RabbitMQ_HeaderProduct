package com.ht.dev.headerp;


import com.rabbitmq.client.*;
import java.util.HashMap;
import java.util.Map;

public class EmitLogHeader {

  private static final String EXCHANGE_NAME = "header_test";

  public static void main(String[] argv) throws Exception {
    if (argv.length < 1) {
      System.err.println("Utilisation: EmitLogHeader message queueName [headers]...");
      System.exit(1);
    }


    String routingKey = "RoutingKeyInutile";

    //argument 0 = message à passer
    String message = argv[0];

    //l'entête est un ensemble de clé/valeur
    Map<String, Object> headers = new HashMap<String, Object>();

    for (int i = 1; i < argv.length; i++) {
      System.out.println("Ajouter entête clé= " + argv[i] + " value= " + argv[i + 1]);
      headers.put(argv[i], argv[i + 1]);
      i++;
    }

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    //utilisation d'un exchange de type routing Headers
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

    //a va creer dans un premier temps un conteneurs de propriétés
    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

    //quelques propriétés standard
    builder.deliveryMode(MessageProperties.PERSISTENT_TEXT_PLAIN.getDeliveryMode());
    builder.priority(MessageProperties.PERSISTENT_TEXT_PLAIN.getPriority());

    //ajouter nos propres propriétés
    builder.headers(headers);

    //construire ces propriètés (assemblage de propriétés AMQP + propriétaires)
    AMQP.BasicProperties theProps = builder.build();

    //publier le message avec les propriétés
    channel.basicPublish(EXCHANGE_NAME, routingKey, theProps, message.getBytes("UTF-8"));
    System.out.println(" [x] Evoyer message: '" + message + "'");
  }
}

