package org.nmdp.servicekafkaproducer.producer;

/**
 * Created by Andrew S. Brown, Ph.D., <andrew@nmdp.org>, on 5/19/17.
 * <p>
 * kafka-producer
 * Copyright (c) 2012-2017 National Marrow Donor Program (NMDP)
 * <p>
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; either version 3 of the License, or (at
 * your option) any later version.
 * <p>
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; with out even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * <p>
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library;  if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA.
 * <p>
 * > http://www.fsf.org/licensing/licenses/lgpl.html
 * > http://www.opensource.org/licenses/lgpl-license.php
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.nmdp.servicekafkaproducer.config.KafkaProducerConfiguration;
import org.nmdp.servicekafkaproducermodel.models.KafkaMessage;

import org.springframework.beans.factory.annotation.Qualifier;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class KafkaMessageProducer {

    private final Producer<byte[], byte[]> producer;
    private final String topic;
    private final Object key;

    public KafkaMessageProducer(@Qualifier("kafkaProducerConfiguration") KafkaProducerConfiguration configuration) {
        producer = new KafkaProducer<>(configuration.getProducerConfiguration());
        key = configuration.getKey();
        topic = configuration.getTopic();
    }

    public void send(List<KafkaMessage> messages) {
        List<ProducerRecord<byte[], byte[]>> records = messages.stream()
                .filter(Objects::nonNull)
                .map(message -> message.convert(topic, key, message))
                .collect(Collectors.toList());

        records.stream()
                .forEach(record -> producer.send(record));

        producer.close();
    }
}
