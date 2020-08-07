package com.curtisnewbie;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.RPCClient" -Dexec.args="#.sys.mem.#"
 */
public class RPCClient {

    private final static String QUEUE_NAME = "rpc_queue";
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";
    private final static String EXCHANGE = "";

    public static void main(String[] args) throws Exception {
        System.out.println("RPC Client Initialised.");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        System.out.println("Main Thread ID: " + Thread.currentThread().getId());
        while (true) {
            // sleep the main thread to avoid keep sending message to RPCServer and
            // receiving response from RPCServer
            Thread.sleep(3000);
            sendTask(channel);
        }
    }

    private static void sendTask(Channel channel) throws Exception {

        // create an id for the message (e.g., unique id of order)
        final String correlationId = UUID.randomUUID().toString();

        // get name of a generated queue, which will be used as a queue for reply
        final String replyQueue = channel.queueDeclare().getQueue();

        // setup correlation id and the queue that the server should reply to
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(correlationId)
                .replyTo(replyQueue).build();

        String task = "Push-Up";
        // publish to the same queue name 'QUEUEblock_NAME' used by the RPCServer
        channel.basicPublish(EXCHANGE, QUEUE_NAME, props, task.getBytes(DEF_ENCODING));

        // block to wait for response
        final BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<>(1);

        // read response from reply queue, a worker thread is created for 'consume' operation
        boolean autoAck = true;
        channel.basicConsume(replyQueue, autoAck, (consumerTag, delivery) -> {
            System.out.println("Worker Thread ID: " + Thread.currentThread().getId());
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                blockingQueue.offer(new String(delivery.getBody(), DEF_ENCODING));
            }
        }, consumerTag -> {
        });

        // this is blocking operation, take response until there is one
        System.out.println("Reply: " + blockingQueue.take());
    }
}
