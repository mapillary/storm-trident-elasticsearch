/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.fhuss.storm.elasticsearch.state;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.TransactionalValue;
import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract class to serialize {@link TransactionalValue}, {@link OpaqueValue}
 * and non transactional value.
 *
 * @author fhussonnois
 *
 * @param <T> type of the document
 */
public abstract class ValueSerializer<T> implements Serializable {

    private static final String FIELD_TXID      = "txid";
    private static final String FIELD_CURR_TIXD = "currTxid";
    private static final String FIELD_VAL       = "val";
    private static final String FIELD_CURR      = "curr";
    private static final String FIELD_PREV      = "prev";

    protected static final ObjectMapper mapper = new ObjectMapper();

    public byte[] serialize(T o) throws IOException {
        return mapper.writeValueAsBytes(o);
    }

    public abstract T deserialize(byte[] value) throws IOException;

    /**
     * Basic serializer implementation for {@link org.apache.storm.trident.state.TransactionalValue}.
     * @param <T> the value type
     */
    public static class NonTransactionalValueSerializer<T> extends ValueSerializer<T> {
        private Class<T> type;
        public NonTransactionalValueSerializer(Class<T> type) {
            this.type = type;
        }


        @Override
        public T deserialize(byte[] value) throws IOException {
            return mapper.readValue(value, type);
        }
    }

    /**
     * Basic serializer implementation for {@link org.apache.storm.trident.state.TransactionalValue}.
     * @param <T> the value type
     */
    public static class TransactionalValueSerializer<T> extends ValueSerializer<TransactionalValue<T>> {

        private Class<T> type;

        public TransactionalValueSerializer(Class<T> type) {
            this.type = type;
        }

        @Override
        public TransactionalValue<T> deserialize(byte[] value) throws IOException {
            ObjectNode node = mapper.readValue(value, ObjectNode.class);
            byte[] bytes = mapper.writeValueAsBytes(node.get(FIELD_VAL));
            return new TransactionalValue<>(node.get(FIELD_TXID).asLong(), mapper.readValue(bytes, type));
        }
    }

    /**
     * Basic serializer implementation for {@link org.apache.storm.trident.state.OpaqueValue}.
     * @param <T> the value type
     */
    public static class OpaqueValueSerializer<T> extends ValueSerializer<OpaqueValue<T>> {

        private Class<T> type;

        public OpaqueValueSerializer(Class<T> type) {
            this.type = type;
        }

        @Override
        public OpaqueValue<T> deserialize(byte[] value) throws IOException {
            ObjectNode node = mapper.readValue(value, ObjectNode.class);
            long currTxid = node.get(FIELD_CURR_TIXD).asLong();
            T val = mapper.readValue(mapper.writeValueAsBytes(node.get(FIELD_CURR)), type);
            JsonNode prevNode = node.get(FIELD_PREV);
            T prev = (prevNode.isNull()) ? null : mapper.readValue(mapper.writeValueAsBytes(prevNode), type);
            return new OpaqueValue<>(currTxid, val, prev);
        }
    }
}
