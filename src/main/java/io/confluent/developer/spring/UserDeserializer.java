package io.confluent.developer.spring;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.developer.spring.model.User;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public User deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();

        User user = null;
        try {
            user = mapper.readValue(bytes, User.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return user;
    }

    @Override
    public User deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
