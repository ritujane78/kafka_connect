package com.jane.kafka.broker.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jane.kafka.broker.message.*;
import com.jane.kafka.broker.schema.KafkaConnectPersonAddressTargetKeySchema;
import com.jane.kafka.broker.schema.KafkaConnectPersonAddressTargetValueSchema;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PersonAddressFromHttpStream {
    private static final Logger LOG = LoggerFactory.getLogger(PersonAddressFromHttpStream.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    void kstreamPersonAddressFromHttp(StreamsBuilder builder) {
        var sourceStream = builder.stream("t-person-address-http", Consumed.with(Serdes.String(), Serdes.String()));
        var targetStream = sourceStream.flatMap(expandPersonAddress());

        targetStream.to("t-person-address-target", Produced.with(Serdes.String(), Serdes.String()));
    }

    private KeyValueMapper<String, String, Iterable<KeyValue<String, String>>> expandPersonAddress() {
        return (key, value) -> {

            var expanded = new ArrayList<KeyValue<String, String>>();

            try {
                String json = value;

                // 🔥 FIX: unwrap double-encoded JSON
                if (json != null && json.startsWith("\"")) {
                    json = objectMapper.readValue(json, String.class);
                }


                List<KafkaConnectPersonMessageSnakeCase> personAddressObjects =
                        objectMapper.readValue(json,
                                new TypeReference<List<KafkaConnectPersonMessageSnakeCase>>() {});

                for (var personAddressObject : personAddressObjects) {
                    for (var address : personAddressObject.getAddresses()) {

                        var targetKey = generateTargetKey(address.getAddressId());
                        var targetValue = generateTargetValue(personAddressObject, address);

                        expanded.add(KeyValue.pair(targetKey, targetValue));
                    }
                }

            } catch (Exception e) {
                LOG.error("Invalid data : {} throws : {}", value, e.getMessage());
            }

            return expanded;
        };
    }
    private String generateTargetKey(int addressId) throws JsonProcessingException {
        var targetKey = new KafkaConnectMessage<Integer>();
        targetKey.setSchema(KafkaConnectPersonAddressTargetKeySchema.instance());
        targetKey.setPayload(addressId);

        return objectMapper.writeValueAsString(targetKey);
    }

    private String generateTargetValue(KafkaConnectPersonMessageSnakeCase person,
                                       KafkaConnectAddressMessageSnakeCase address) throws JsonProcessingException {
        var targetPayload = new KafkaConnectPersonTargetMessage();

        targetPayload.setEmail(person.getEmail());
        targetPayload.setPersonId(person.getPersonId());
        targetPayload.setFullName(person.getFullName());
        targetPayload.setAddress(address.getAddress());
        targetPayload.setCity(address.getCity());
        targetPayload.setPostalCode(address.getPostalCode());
        targetPayload.setAddressId(address.getAddressId());

        var targetValue = new KafkaConnectMessage<KafkaConnectPersonTargetMessage>();

        targetValue.setSchema(KafkaConnectPersonAddressTargetValueSchema.instance());
        targetValue.setPayload(targetPayload);

        return objectMapper.writeValueAsString(targetValue);
    }

}
