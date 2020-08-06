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
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.Worker"
 */
public class Worker {

    private final static String QUEUE_NAME = "task_queue";
    private final static String HOST = "localhost";
    private final static String DEF_ENCODING = "UTF-8";
    private final static int PRE_FETCH_COUNT = 1; // one message per consumer at a time

    public static void main(String[] args) throws Exception {
        System.out.println("Worker Initialised.");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        // arguments to declare queue
        boolean durable = true; // queue durability
        boolean exclusiveToConnection = false;
        boolean autoDeleteQueue = false;
        Map<String, Object> extraArg = null;

        // declare the same queue with the server
        channel.queueDeclare(QUEUE_NAME, durable, exclusiveToConnection, autoDeleteQueue, extraArg);

        // fetch one message at a time, to avoid blindly fetching all available messages
        channel.basicQos(PRE_FETCH_COUNT);

        System.out.println(" [*] Waiting for task.");

        // defind callback for received messages, each worker will receive different messages
        // the messages are evenly dispatched (aka, fair dispatch) using round-robin
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), DEF_ENCODING);
            finishTask(msg);
            // manual message acknowledgement
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.printf(" [x] Received task '%s'\n", msg);
        };
        CancelCallback cancelCallback = (consumerTag) -> {
            System.out.printf(" [x] Task cancelled '%d'\n", consumerTag);
        };

        boolean msgAcknowledgement = false;
        // set to true, then message acknowledgement triggers messsage (sent)
        // deletion, and if current worker dies, the task (msg)
        // is not lost, (see manual acknowledgement above)
        channel.basicConsume(QUEUE_NAME, msgAcknowledgement, deliverCallback, cancelCallback);
    }

    private static void finishTask(String task) {
        for (char ch : task.toCharArray())
            try {
                if (ch == '.')
                    Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }
}
