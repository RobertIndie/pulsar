/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
public class ConsumerReadOnlyTest extends ProducerConsumerBase {
    private static final Logger log = LoggerFactory.getLogger(ConsumerReadOnlyTest.class);
    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setBookkeeperExplicitLacIntervalInMills(100);
        super.internalSetup();
        super.producerBaseSetup();
        admin.namespaces().createNamespace("public/default-shadow");
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testReadOnly() throws Exception {
        String topic = "persistent://public/default/test";
        String subName = "my-sub";
        @Cleanup
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("my-producer-name")
                .create();
        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionProperties(Collections.singletonMap("ReadOnly", "true"))
                .subscribe();

        // The shadow topic is created asynchronously. We need to wait some time to let it be created.
        Thread.sleep(1000);

         producer.send("Hello");

        Message<String> msg = consumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "Hello");

        TopicStats srcTopicStats = admin.topics().getStats(topic);
        assertTrue(srcTopicStats.getSubscriptions().isEmpty());
        assertEquals(srcTopicStats.getMsgOutCounter(), 0);

        String shadowTopic = "persistent://public/default-shadow/test";
        TopicStats shadowTopicStats = admin.topics().getStats(shadowTopic);
        assertFalse(shadowTopicStats.getSubscriptions().isEmpty());
        assertTrue(shadowTopicStats.getSubscriptions().containsKey(subName));

        @Cleanup
        Consumer<String> shadowConsumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(shadowTopic)
                .subscriptionName("shadow-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<String> shadowMsg = shadowConsumer.receive(3, TimeUnit.SECONDS);
        assertNotNull(shadowMsg);

        try {
            Producer<String> shadowProducer = pulsarClient.newProducer(Schema.STRING)
                    .topic(shadowTopic)
                    .create();
            shadowProducer.send("test");
            fail("Should fail");
        } catch (PulsarClientException.NotAllowedException e) {
            assertEquals(e.getMessage(), "Cannot send messages to a shadow topic");
        }
    }
}
