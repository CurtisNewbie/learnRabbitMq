package com.curtisnewbie;

import java.util.Date;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.RPCServer"
 * <p>
 * Don't use RPC, it's synchronous
 */
public class RPCServer {
    private final static String HOST = "localhost";
    private final static String QUEUE_NAME = "rpc_queue";
    private final static String DEF_ENCODING = "UTF-8";
    private final static String EXCHANGE = "";

    public static void main(String[] args) throws Exception {
        System.out.println("Main Thread ID: " + Thread.currentThread().getId());
        System.out.println("RPCServer Initialised\nAccepting RPC...");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel();) {

            // create non-durable, auto-delete queue
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queuePurge(QUEUE_NAME); // purge the content for now to avoid missing incoming
                                            // RPC calls

            System.out.println(" [x] Awaiting PRCs requests");

            // use a lock to synchronise request handling, i.e., block the main thread,
            // such that the main thread doesn't exit, while worker thread (used by
            // deliveryCallback) is not blocked
            Object monitor = new Object();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                System.out.println(
                        "DeliverCallback Worker Thread ID: " + Thread.currentThread().getId());
                try {
                    // set up properties/ correlation id for reply
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                            .correlationId(delivery.getProperties().getCorrelationId()).build();
                    // mimic RPC operation
                    String task = new String(delivery.getBody(), DEF_ENCODING);
                    String response = complete(task);

                    // reply response, replyTo is a name of a queue provided by RPCClient
                    // and this replyTo queue name is used as routing key for routing
                    String replyTo = delivery.getProperties().getReplyTo();
                    channel.basicPublish(EXCHANGE, replyTo, replyProps,
                            response.getBytes(DEF_ENCODING));
                    // message acknowledgement
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumeTag -> {
            });

            // keep the main thread blocked and running
            while (true) {
                synchronized (monitor) {
                    try {
                        System.out.println("Main Thread Blocked");
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static String complete(String task) {
        System.out.println("Received RPC.");
        return String.format("You did '%s' at: %s", task, new Date().toString());
    }
}


