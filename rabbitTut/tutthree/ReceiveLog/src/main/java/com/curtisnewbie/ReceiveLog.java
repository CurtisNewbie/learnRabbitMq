package com.curtisnewbie;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.ReceiveLog"
 */
public class ReceiveLog {
    private final static String EXCHANGE_NAME = "logs";
    private final static String EXCHANGE_TYPE = "fanout";
    private final static String ROUTING_KEY = ""; // routing key is ignored in fanout
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";
    private final static int PRE_FETCH_COUNT = 1; // one message per consumer at a time

    public static void main(String[] args) throws Exception {
        System.out.println("Msgs Receiver Initialised.");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // queue name isn't important with fanout
        // get name of a generated queue, since we are using fanout, it doesn't matter
        String tempQueueName = channel.queueDeclare().getQueue();

        // bind exchange to the queue we are using, as if we are subscribing
        // routing key is ignored since we are using fanout
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        channel.queueBind(tempQueueName, EXCHANGE_NAME, ROUTING_KEY);

        // ! no need to declare a queue, we just use the one generated
        // channel.queueDeclare(tempQueueName, durable, exclusiveToConnection, autoDeleteQueue,
        // extraArg);

        // fetch one message at a time, to avoid blindly fetching all available messages
        channel.basicQos(PRE_FETCH_COUNT);

        System.out.println(" [*] Waiting for logs.");

        // defind callback for received messages, each worker will receive different messages
        // the messages are evenly dispatched (aka, fair dispatch) using round-robin
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), DEF_ENCODING);
            System.out.printf(" [x] Received log '%s'\n", msg);
        };
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.printf(" [x] Log cancelled '%d'\n", consumerTag);
        };

        boolean msgAck = true;
        channel.basicConsume(tempQueueName, msgAck, deliverCallback, cancelCallback);
    }
}
