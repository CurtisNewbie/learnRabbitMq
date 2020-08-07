package com.curtisnewbie;

import java.util.Random;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.BasicProperties;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.EmitLog"
 */
public class EmitLog {
    private static String tempQueueName; // queue name isn't very important with fanout
    private final static String HOST = "localhost";
    private final static String EXCHANGE_NAME = "logs";
    private final static String EXCHANGE_TYPE = "fanout";
    private final static String ROUTING_KEY = ""; // routing key is ignored in fanout
    private final static String RANDOM_QUEUE_NAME = "";

    /**
     * With fanout, we are sending messages to exchange, and any queues that are bound to the
     * exchange will receive the published messages.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Log Emitter Initialised\nEmitting logs with Fanout (broadcasting)...");
        ConnectionFactory factory = new ConnectionFactory();
        // host ip or name
        factory.setHost(HOST);
        // open connection to RabbitMQ
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel();) {

            // declare an exchange with fanout type (broadcasting to all channels)
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

            // get name of a generated queue, since we are using fanout, it doesn't matter.
            // plus this queue is automatically deleted once we are done, it's
            // exclusive, non-durable, new/fresh and empty
            tempQueueName = channel.queueDeclare().getQueue();

            // bind exchange to the queue we are using
            channel.queueBind(tempQueueName, EXCHANGE_NAME, ROUTING_KEY);

            // ! no need to declare a queue, we just use the one generated
            // channel.queueDeclare(tempQueueName, durable, exclusiveToConnection, autoDeleteQueue,
            // extraArg);

            // fanout / pub-sub
            for (int i = 0; i < 5; i++) {
                BasicProperties properties = null; // no need for msg persistence
                String randomMsg = randomLog();
                channel.basicPublish(EXCHANGE_NAME, RANDOM_QUEUE_NAME, properties,
                        randomMsg.getBytes());
                System.out.printf("[x] Sent: '%s'\n", randomMsg);
            }
        }
    }

    private static String randomLog() {
        Random rand = new Random();
        // 0 - 3 seconds
        StringBuilder sb = new StringBuilder("log_");
        int len = 10;
        for (int i = 0; i < len; i++) {
            sb.append((char) (rand.nextInt(10) + 'a'));
        }
        return sb.toString();
    }
}


