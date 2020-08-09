package com.curtisnewbie;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


/**
 * To run this program:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.curtisnewbie.PublishConfirm"
 */
public class PublisherConfirm {

    private final static String HOST = "localhost";
    private final static String QUEUE_NAME = "publisherConfirm";
    private final static String EXCHANGE = "";
    private final static ConcurrentNavigableMap<Long, String> publishedMsgs =
            new ConcurrentSkipListMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("Publisher Confirm DEMO.");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        // Enable Publisher Confirm on Channel (only for once)
        // what it means is that when we publish a message, we expects a confirmation
        // from the client/consumer side, we can make it a blocking/async operation, and
        // wait for the confirmation
        channel.confirmSelect();

        String msg = "random stuff";
        System.out.println("Sync Individual Ack");
        // block and wait for the confirm from consumer within timeout
        confirmEach(channel, msg);
        System.out.println("Sync Batch Ack");
        // blcok and wait for confirmation for a batch of msgs
        confirmBatch(channel, msg);
        System.out.println("Async Batch Ack");
        // wait for confirmation asynchronously with callbacks
        asyncConfirm(channel, msg);
    }

    // Individual
    private static void confirmEach(Channel channel, String m)
            throws IOException, TimeoutException, InterruptedException {
        String msg = m + "_each";
        channel.basicPublish(EXCHANGE, QUEUE_NAME, null, msg.getBytes());
        // block and wait for the confirm from consumer
        // if within timeout, the msg isn't confirmed, a
        // TimeoutException is thrown indicating that some sort of issue
        // occrued, and the msg isn't consumed properly
        channel.waitForConfirmsOrDie(5000);
    }

    // Batch
    private static void confirmBatch(Channel channel, String m)
            throws IOException, TimeoutException, InterruptedException {
        String msg = m + "_batch";
        // still a blocking operation, except that it send the messages
        // in batches, and wait for a single confirmation for the batch,
        // which drasitcally improves throughput. But it also indicates
        // the difficult handling of failure, e.g., we don't konw which
        // message is not confirmed, and how to handle the failure for
        // the whole batch
        int msgCount = 5;
        for (int i = 0; i < msgCount; i++) {
            channel.basicPublish(EXCHANGE, QUEUE_NAME, null, msg.getBytes());
        }
        channel.waitForConfirmsOrDie(5000);
    }

    private static void asyncConfirm(Channel channel, String m)
            throws IOException, TimeoutException, InterruptedException {
        String msg = m + "_async";
        // register callbacks for acknowledgement and non-ackoowledgement
        channel.addConfirmListener(PublisherConfirm::acknowledged,
                PublisherConfirm::notAcknowledged);

        for (int i = 0; i < 5; i++) {
            // we use the sequenceNo to uniquely identify each msg
            long segId = channel.getNextPublishSeqNo();
            String body = msg + i;
            publishedMsgs.put(segId, body);
            channel.basicPublish(EXCHANGE, QUEUE_NAME, null, (body).getBytes());
        }
    }

    private static void acknowledged(long seqId, boolean multiple) {
        if (multiple) {
            // items before seqId (less than or equal to) are all confirmed (this is why it's called
            // multiple)
            ConcurrentNavigableMap<Long, String> confirmed = publishedMsgs.headMap(seqId, true);
            for (Entry<Long, String> entry : confirmed.entrySet()) {
                System.out.printf("Msg: %s, confirmed/acknowledged, multiple: %b\n",
                        entry.getValue(), multiple);
                publishedMsgs.remove(entry.getKey());
            }
        } else {
            System.out.printf("Msg: %s, confirmed/acknowledged, multiple: %b\n",
                    publishedMsgs.get(seqId), multiple);
            publishedMsgs.remove(seqId);
        }
    }

    private static void notAcknowledged(long seqId, boolean multiple) {
        if (multiple) {
            // items before seqId (less than or equal to) are all n-ecked
            ConcurrentNavigableMap<Long, String> necked = publishedMsgs.headMap(seqId, true);
            for (Entry<Long, String> entry : necked.entrySet()) {
                System.out.printf("Msg: %s, not confirmed/acknowledged\n", entry.getValue());
                publishedMsgs.remove(entry.getKey());
            }
        } else {
            System.out.printf("Msg: %s, not confirmed/acknowledged\n", publishedMsgs.get(seqId));
            publishedMsgs.remove(seqId);
        }
    }
}

