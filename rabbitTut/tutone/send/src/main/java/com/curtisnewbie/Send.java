package com.curtisnewbie;

import java.util.Map;
import java.util.Optional;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.BasicProperties;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.Send"
 */
public class Send {
    private final static String QUEUE_NAME = "hello";
    private final static String HOST = "localhost";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        // host ip or name
        factory.setHost(HOST);
        // open connection to RabbitMQ
        try (Connection conn = factory.newConnection()) {
            // obtain a channel
            Optional<Channel> opt = conn.openChannel();
            if (opt.isEmpty()) {
                System.err.println("Connection not opened");
                return;
            } else {
                // channel
                Channel chan = opt.get();

                // arguments to declare queue
                boolean durable = false;
                boolean exclusiveToConnection = false;
                boolean autoDeleteQueue = false;
                Map<String, Object> extraArg = null;

                // declare a queue
                chan.queueDeclare(QUEUE_NAME, durable, exclusiveToConnection, autoDeleteQueue,
                        extraArg);

                String msg = "Hello World";
                String exchange = "";
                BasicProperties properties = null;
                chan.basicPublish(exchange, QUEUE_NAME, properties, msg.getBytes());
                System.out.printf("[x] Sent: '%s'\n", msg);
            }
        }
        System.out.println("Program terminated");
    }

}


