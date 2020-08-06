package com.curtisnewbie;

import java.util.Map;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.Receive"
 */
public class Receive {

    private final static String QUEUE_NAME = "hello";
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // arguments to declare queue
        boolean durable = false;
        boolean exclusiveToConnection = false;
        boolean autoDeleteQueue = false;
        Map<String, Object> extraArg = null;

        // declare the same queue with the server
        channel.queueDeclare(QUEUE_NAME, durable, exclusiveToConnection, autoDeleteQueue, extraArg);
        System.out.println(" [*] Waiting for msgs.");

        // defind callback for received messages
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), DEF_ENCODING);
            System.out.printf(" [x] Received '%s'\n", msg);
        };
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.printf(" [x] Message cancelled '%d'\n", consumerTag);
        };
        boolean msgAcknowledgement = true;
        channel.basicConsume(QUEUE_NAME, msgAcknowledgement, deliverCallback, cancelCallback);
    }
}
