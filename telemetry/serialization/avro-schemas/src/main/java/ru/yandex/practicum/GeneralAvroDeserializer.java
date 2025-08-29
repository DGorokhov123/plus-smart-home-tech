package ru.yandex.practicum;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

public class GeneralAvroDeserializer implements Deserializer<SpecificRecordBase> {

    private final DecoderFactory decoderFactory = DecoderFactory.get();

    @Override
    public SpecificRecordBase deserialize(String topic, byte[] bytes) {
        if (bytes == null) return null;

        try {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(bytes, null);
            DatumReader<SpecificRecordBase> reader = switch (topic) {
                case "telemetry.sensors.v1" -> new SpecificDatumReader<>(SensorEventAvro.getClassSchema());
                case "telemetry.hubs.v1" -> new SpecificDatumReader<>(HubEventAvro.getClassSchema());
                default -> throw new IllegalArgumentException("Неизвестный топик: " + topic);
            };
            return reader.read(null, decoder);
        } catch (Exception e) {
            throw new SerializationException("Deserialization Error for topic " + topic, e);
        }
    }

}
