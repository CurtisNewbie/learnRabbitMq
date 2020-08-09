package com.curtisnewbie;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.MsgConfirm"
 * <p>
 * Don't use RPC, it's synchronous
 */
public class MsgConfirm {
    private final static String HOST = "localhost";
    private final static String QUEUE_NAME = "publisherConfirm";
    private final static String DEF_ENCODING = "UTF-8";
    private final static String EXCHANGE = "";

    public static void main(String[] args) throws Exception {
        System.out.println("Main Thread ID: " + Thread.currentThread().getId());
        System.out.println("Msg confirm initialised, will confirm all messages");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, true, null);
        channel.basicQos(1);

        DeliverCallback consume = (consumerTag, msg) -> {
            long tag = msg.getEnvelope().getDeliveryTag();
            channel.basicAck(tag, true);
            System.out.printf("Acknowledged '%s'\n", new String(msg.getBody(), DEF_ENCODING));
        };

        channel.basicConsume(QUEUE_NAME, false, consume, (consumerTag) -> {
            // do nothing for cancelled msgs
        });
    }
}


