package com.curtisnewbie;

import java.util.Random;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.EmitLogDirect"
 */
public class EmitLogDirect {
    private final static String HOST = "localhost";
    private final static String EXCHANGE_NAME = "direct_logs";
    private final static String EXCHANGE_TYPE = "direct";
    private static String severity; // log msg severity is used as routing key
    private final static String[] severities = new String[] {"severe", "info", "error", "warning"};

    /**
     * With direct, we are sending messages to exchange which then routes messages to queues based
     * on routing key, and any queues that are bound to the exchange (with the routing key) will
     * receive the published messages.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Direct Log Emitter Initialised\nEmitting logs with direct...");

        // use severity level as routing key
        severity = randLevel();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel();) {
            // declare an exchange with direct type (broadcasting to queues bound with same routing
            // key)
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

            for (int i = 0; i < 5; i++) {
                String randomMsg = randomLog();
                // severity level as routing key
                channel.basicPublish(EXCHANGE_NAME, severity, null, randomMsg.getBytes());
                System.out.printf("[x][%s] Sent: '%s'\n", severity.toUpperCase(), randomMsg);
            }
        }
    }

    private static String randLevel() {
        return severities[new Random().nextInt(Integer.MAX_VALUE) % severities.length];
    }

    private static String randomLog() {
        Random rand = new Random();
        // 0 - 3 seconds
        StringBuilder sb = new StringBuilder(severity);
        sb.append("_log_");
        int len = 10;
        for (int i = 0; i < len; i++) {
            sb.append((char) (rand.nextInt(10) + 'a'));
        }
        return sb.toString();
    }
}


