/*
    Copyright 2014 Benjamin Black

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package us.b3k.kafka.ws.consumer;

//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.b3k.kafka.ws.messages.AbstractMessage;
import us.b3k.kafka.ws.messages.BinaryMessage;
import us.b3k.kafka.ws.messages.TextMessage;
import us.b3k.kafka.ws.transforms.Transform;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;

import javax.websocket.CloseReason;
import javax.websocket.RemoteEndpoint.Async;
import javax.websocket.Session;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkawebsocketConsumer implements Runnable  {
    private static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

  //  private final ExecutorService executorService;
    private final Transform transform;
    private final Session session;
    private final Properties properties;
   // private ConsumerConnector connector;
    private final List<String> topics;
    private final Async remoteEndpoint;
    private final CountDownLatch shutdownLatch;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    public KafkawebsocketConsumer(Properties configProps, final ExecutorService executorService, final Transform transform, final String topics, final Session session) {
        this.remoteEndpoint = session.getAsyncRemote();
        this.properties = configProps;
       // this.executorService = executorService;
        this.topics = Arrays.asList(topics.split(","));
        this.transform = transform;
        this.session = session;
        this.shutdownLatch = new CountDownLatch(1);
        consumer = new KafkaConsumer<String, String>(properties);
    }

    public KafkawebsocketConsumer(Properties consumerConfig, final ExecutorService executorService, final Transform transform, final List<String> topics, final Session session) {
        this.remoteEndpoint = session.getAsyncRemote();
        this.properties = consumerConfig;
      //  this.executorService = executorService;
        this.topics = topics;
        this.transform = transform;
        this.session = session;
        this.shutdownLatch = new CountDownLatch(1);
        consumer = new KafkaConsumer<String, String>(properties);
    }

    public void start() {
//        LOG.debug("Starting consumer for {}", session.getId());
//        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
//       // this.connector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
//
////        Map<String, Integer> topicCountMap = new HashMap<>();
////        for (String topic : topics) {
////            topicCountMap.put(topic, 1);
////        }
//        consumer.subscribe(topics);
//        
//        //Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = connector.createMessageStreams(topicCountMap);
//
////        for (String topic : topics) {
////            LOG.debug("Adding stream for session {}, topic {}",session.getId(), topic);
////            final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
////            for (KafkaStream<byte[], byte[]> stream : streams) {
//                executorService.submit(new KafkaConsumerTask(consumer, remoteEndpoint, transform, session));
//           // }
//        //}
    }
    @Override
    @SuppressWarnings("unchecked")
    public void run() {
    	
        String subprotocol = session.getNegotiatedSubprotocol();
        
        consumer.subscribe(topics);
      //  for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : (Iterable<MessageAndMetadata<byte[], byte[]>>) stream) {
        try
        {
        while (!closed.get()) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            //records.forEach(record -> process(record));
            consumer.commitAsync();
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            String topic = record.topic();
            String message =record.value();
            
            switch(subprotocol) {
                case "kafka-binary":
                 //   sendBinary(topic, message);
                    break;
                default:
                    sendText(topic, message);
                    break;
            }
           
          }
        }
        } catch (WakeupException e) {
          // ignore, we're closing
        	//if (!closed.get()) throw e;
        } catch (Exception e) {
          //log.error("Unexpected error", e);
        } finally {
          consumer.close();
          shutdownLatch.countDown();
        }
        
        
            
            if (Thread.currentThread().isInterrupted()) {
                try {
                    session.close();
                } catch (IOException e) {
                    LOG.error("Error terminating session: {}", e.getMessage());
                }
                return;
            }
        }
    //}
    private void sendBinary(String topic, byte[] message) {
        AbstractMessage msg = transform.transform(new BinaryMessage(topic, message), session);
        if(!msg.isDiscard()) {
            remoteEndpoint.sendObject(msg);
        }
    }

    private void sendText(String topic, String messageString) {
        //String messageString = new String(message, Charset.forName("UTF-8"));
        LOG.trace("XXX Sending text message to remote endpoint: {} {}", topic, messageString);
        AbstractMessage msg = transform.transform(new TextMessage(topic, messageString), session);
        if(!msg.isDiscard()) {
            remoteEndpoint.sendObject(msg);
        }
    }
    public void stop() {
        LOG.info("Stopping consumer for session {}", session.getId());
//       if (consumer != null) {
//    	   consumer.unsubscribe();
//       	consumer.close();
//           shutdownLatch.countDown();
//        }
        closed.set(true);
        consumer.wakeup();
        LOG.info("Stopped consumer for session {}", session.getId());
    }

//    static public class KafkaConsumerTask {
//        private KafkaConsumer<String, String> consumer;
//        private Async remoteEndpoint;
//        private final Transform transform;
//        private final Session session;
//        private final CountDownLatch shutdownLatch;
//        public KafkaConsumerTask(KafkaConsumer<String, String> stream, Async remoteEndpoint,
//                                 final Transform transform, final Session session) {
//            this.consumer = stream;
//            this.remoteEndpoint = remoteEndpoint;
//            this.transform = transform;
//            this.session = session;
//            this.shutdownLatch = new CountDownLatch(1);
//        }
//
//       
//      
//        public void shutdown() throws InterruptedException {
//        	consumer.unsubscribe();
//            consumer.close();
//            shutdownLatch.countDown();
//            
//          }
//        private void closeSession(Exception e) {
//            LOG.debug("Consumer initiated close of session {}", session.getId());
//            try {
//                session.close(new CloseReason(CloseReason.CloseCodes.CLOSED_ABNORMALLY, e.getMessage()));
//            } catch (IOException ioe) {
//                LOG.error("Error closing session: {}", ioe.getMessage());
//            }
//        }
//    }
}
