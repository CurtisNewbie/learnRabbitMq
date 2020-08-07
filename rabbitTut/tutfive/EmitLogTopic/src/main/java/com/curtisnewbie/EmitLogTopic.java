package com.curtisnewbie;

import java.util.Random;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.EmitLogTopic" -Dexec.args="sys mem cpu"
 * <p>
 * "sys.mem" won't match because the incorrect number of words
 * <p>
 * "sys.mem.#" matches
 * <p>
 * "#.sys.mem.#" matches
 * <p>
 * "sys.mem.cpu" matches
 * <p>
 */
public class EmitLogTopic {
    private final static String HOST = "localhost";
    private final static String EXCHANGE_NAME = "topic_logs";
    private final static String EXCHANGE_TYPE = "topic";

    // Topic exchange is more or less like an advancement on top of direct exchange, where simple
    // routing key is replaced with words (topics) delimitered by '.'
    // the * is for one word (topic), and # is for zero or many words (topic)
    // e.g, *.banana.orange.# (one word before banana, banana, orange, and any words after
    // orange)
    public static void main(String[] args) throws Exception {
        System.out.println("Topic Log Emitter Initialised\nEmitting logs with topic...");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel();) {

            // declare an exchange with topic type (broadcasting to queues bound with same routing
            // key) with topics
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

            String topics = args.length > 0 ? parseTopics(args) : "default";
            System.out.printf("Topics of message: %s\n", topics);

            for (int i = 0; i < 5; i++) {
                String randomMsg = randomLog();
                // topics as routing key
                channel.basicPublish(EXCHANGE_NAME, topics, null, randomMsg.getBytes());
                System.out.printf("[x] Sent: '%s'\n", randomMsg);
            }
        }
    }

    private static String parseTopics(String[] args) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            sb.append(args[i]).append('.');
        }
        return sb.deleteCharAt(sb.length() - 1).toString();
    }

    private static String randomLog() {
        Random rand = new Random();
        // 0 - 3 seconds
        StringBuilder sb = new StringBuilder();
        sb.append("log_");
        int len = 10;
        for (int i = 0; i < len; i++) {
            sb.append((char) (rand.nextInt(10) + 'a'));
        }
        return sb.toString();
    }
}


