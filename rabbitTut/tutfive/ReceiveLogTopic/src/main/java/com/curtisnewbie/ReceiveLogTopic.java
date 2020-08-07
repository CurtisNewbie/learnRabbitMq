package com.curtisnewbie;

import java.util.*;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.ReceiveLogTopic" -Dexec.args="#.sys.mem.#"
 */
public class ReceiveLogTopic {
    private final static String EXCHANGE_NAME = "topic_logs";
    private final static String EXCHANGE_TYPE = "topic";
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";
    private final static int PRE_FETCH_COUNT = 1; // one message per consumer at a time

    public static void main(String[] args) throws Exception {
        System.out.println("Topic Log Receiver Initialised.");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // Topic is more or less like an advancement on top of direct exchange with routing keys
        // the * is for one word (topic), and # is for zero or many words (topic)
        // e.g, *.banana.orange.# (one word before banana, banana, orange, and any words after
        // orange), we can also register multiple topics (patterns)
        String[] topics = new String[args.length > 0 ? args.length : 1];
        if (args.length == 0)
            topics[0] = "#"; // default is to match all topics
        else
            for (int i = 0; i < args.length; i++)
                topics[i] = args[i];

        System.out.printf("Listening to topics: %s\n", Arrays.toString(topics));

        // get name of a generated queue
        String tempQueueName = channel.queueDeclare().getQueue();

        // bind exchange to the topic pattern that we are interested, as if we are subscribing to
        // multiple topic patterns
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        for (String topicPattern : topics)
            channel.queueBind(tempQueueName, EXCHANGE_NAME, topicPattern);

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
