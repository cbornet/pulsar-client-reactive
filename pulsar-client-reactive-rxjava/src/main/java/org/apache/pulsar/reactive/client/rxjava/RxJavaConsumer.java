/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.reactive.client.rxjava;

import java.util.function.Function;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.reactive.client.api.MessageResult;
import org.apache.pulsar.reactive.client.api.ReactiveMessageConsumer;
import org.reactivestreams.Publisher;

/**
 * An RxJava wrapper for a {@link ReactiveMessageConsumer}.
 */
public class RxJavaConsumer<T> {

	private final ReactiveMessageConsumer<T> consumer;

	/**
	 * Create a new {@link RxJavaConsumer} wrapping the given
	 * {@link ReactiveMessageConsumer}.
	 * @param consumer the consumer to wrap
	 */
	public RxJavaConsumer(ReactiveMessageConsumer<T> consumer) {
		this.consumer = consumer;
	}

	/**
	 * Consume one message.
	 * @param messageHandler a {@link Function} to apply to the consumed message that
	 * returns a {@link MessageResult} which contains the acknowledgement or negative
	 * acknowledgement referencing the message id of the input message together with an
	 * optional return value object
	 * @param <R> the type of MessageResult returned by the message handler
	 * @return the value contained by the {@link MessageResult} returned by the message
	 * handler
	 */
	public <R> Single<R> consumeOne(Function<Message<T>, Single<MessageResult<R>>> messageHandler) {
		return Single.fromPublisher(this.consumer.consumeOne((m) -> messageHandler.apply(m).toFlowable()));
	}

	/**
	 * Consume messages continuously.
	 * @param messageHandler a {@link Function} to apply to the consumed messages that
	 * returns {@link MessageResult}s, each containing the acknowledgement or negative
	 * acknowledgement referencing the message id of the corresponding input messages
	 * together with an optional return value object
	 * @param <R> the type of MessageResult returned by the message handler
	 * @return the values contained by the {@link MessageResult}s returned by the message
	 * handler
	 */
	public <R> Flowable<R> consumeMany(Function<Flowable<Message<T>>, Publisher<MessageResult<R>>> messageHandler) {
		return Flowable
				.fromPublisher((this.consumer.consumeMany((m) -> messageHandler.apply(Flowable.fromPublisher(m)))));
	}

}
