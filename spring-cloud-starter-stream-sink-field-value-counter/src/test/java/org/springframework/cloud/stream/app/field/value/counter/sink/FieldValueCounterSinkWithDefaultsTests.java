/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.stream.app.field.value.counter.sink;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.analytics.metrics.FieldValueCounterRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.app.test.redis.RedisTestSupport;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Ilayaperumal Gopinathan
 * @author Glenn Renfro
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest({"server.port:-1", "field-value-counter.fieldName:test", "spring.metrics.export.includes:"})
@DirtiesContext
@TestPropertySource(properties = "spring.application.name:field-value-counter")
public class FieldValueCounterSinkWithDefaultsTests {

	@Rule
	public RedisTestSupport redisTestSupport = new RedisTestSupport();

	// default field value counter name
	private static final String FVC_NAME = "field-value-counter";

	@Autowired
	private Sink sink;

	@Autowired
	private FieldValueCounterRepository fieldValueCounterRepository;

	@Before
	@After
	public void clear() {
		fieldValueCounterRepository.reset(FVC_NAME);
	}

	@Test
	public void testFieldNameIncrement() {
		assertNotNull(this.sink.input());
		Message<byte[]> message = MessageBuilder.withPayload("{\"test\": \"Hi\"}".getBytes()).build();
		sink.input().send(message);
		message = MessageBuilder.withPayload("{\"test\": \"Hello\"}".getBytes()).build();
		sink.input().send(message);
		message = MessageBuilder.withPayload("{\"test\": \"Hi\"}".getBytes()).build();
		sink.input().send(message);
		assertEquals(2, this.fieldValueCounterRepository.findOne(FVC_NAME).getFieldValueCounts().get("Hi").longValue());
		assertEquals(1, this.fieldValueCounterRepository.findOne(FVC_NAME).getFieldValueCounts().get("Hello").longValue());
	}

}
