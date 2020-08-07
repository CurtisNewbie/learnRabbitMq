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
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.ReceiveLogDirect" -Dexec.args="info severe"
 */
public class ReceiveLogDirect {
    private final static String EXCHANGE_NAME = "direct_logs";
    private final static String EXCHANGE_TYPE = "direct";
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";
    private final static int PRE_FETCH_COUNT = 1; // one message per consumer at a time
    private final static Set<String> severities = new HashSet<>();
    private final static List<String> subscribed = new ArrayList<>();
    static {
        for (String w : new String[] {"severe", "info", "error", "warning"})
            severities.add(w);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Log Receiver Initialised.");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // get name of a generated queue
        String tempQueueName = channel.queueDeclare().getQueue();

        // bind exchange to the queues we are using, as if we are subscribing to multiple direct
        // exchange
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        parseSeverityConfig(args);
        for (String level : subscribed) {
            channel.queueBind(tempQueueName, EXCHANGE_NAME, level);
            System.out.printf("Subscribed to level [%s]\n", level);
        }

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

    private static void parseSeverityConfig(String[] args) {
        for (String s : args) {
            if (severities.contains(s))
                subscribed.add(s); // may contain duplicates
        }
    }
}
