package com.curtisnewbie;

import java.util.Map;
import java.util.Random;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.AMQP.BasicProperties;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.NewTask"
 */
public class NewTask {
    private final static String QUEUE_NAME = "task_queue";
    private final static String HOST = "localhost";

    public static void main(String[] args) throws Exception {
        System.out.println("Task Dispatcher Initialised\nDispatching task...");
        ConnectionFactory factory = new ConnectionFactory();
        // host ip or name
        factory.setHost(HOST);
        // open connection to RabbitMQ
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel();) {
            // arguments to declare queue
            boolean durable = true; // durable queues
            boolean exclusiveToConnection = false;
            boolean autoDeleteQueue = false;
            Map<String, Object> extraArg = null;

            // declare a queue
            channel.queueDeclare(QUEUE_NAME, durable, exclusiveToConnection, autoDeleteQueue,
                    extraArg);

            String task = genRandomTask();
            String exchangeName = ""; // name of direct exchange

            // persistent messages
            BasicProperties properties = MessageProperties.PERSISTENT_TEXT_PLAIN;

            // with default exhcange (direct), when a msg being published and there are more then
            // one consumer, the msgs are evenly dispatched, i.e., it's not pub-sub
            // consumers won't receive all messages published
            // it's called fair dispatch using round-robin
            channel.basicPublish(exchangeName, QUEUE_NAME, properties, task.getBytes());
            System.out.printf("[x] Dispatched Task: '%s'\n", task);
        }
    }

    private static String genRandomTask() {
        // 0 - 3 seconds
        StringBuilder sb = new StringBuilder("Task");
        int sec = new Random().nextInt(4);
        for (int i = 0; i < sec; i++) {
            sb.append('.');
        }
        return sb.toString();
    }
}


