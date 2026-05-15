package com.jane.kafka.broker.schema;

import com.jane.kafka.broker.message.KafkaConnectSchema;
import com.jane.kafka.broker.message.KafkaConnectSchema.Type;

public class KafkaConnectPersonAddressTargetKeySchema {

    private static KafkaConnectSchema instance;

    static {
        instance = new KafkaConnectSchema(Type.int32.toString(), false, null, null);
    }

    private KafkaConnectPersonAddressTargetKeySchema() {
    }

    public static KafkaConnectSchema instance() {
        return instance;
    }

}
