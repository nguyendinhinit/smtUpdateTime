package vn.bnh.single.messages.transform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;


public abstract class UpdateTime<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger logger = LoggerFactory.getLogger(UpdateTime.class);
    private static final String OVERVIEW_DOC = "Insert a upd_tm to current GMT+07 timezone";

    private interface ConfigName {
        String UPD_TIME_FIELD_NAME = "Field Name";
        String UPD_TIME_DEFAULT_VALUE = "upd_tm";
        String TIME_ZONE = "Time Zone";
        String TIME_ZONE_DEFAULT_VALUE="Asia/Ho_Chi_Minh";
        String PATTERN_FORMAT_FIELD_NAME="Date Pattern";
        String PATTERN_FORMAT_DEFAULT_VALUE="yyyy-MM-dd HH:mm:ss";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(ConfigName.UPD_TIME_FIELD_NAME, ConfigDef.Type.STRING, ConfigName.UPD_TIME_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field name for update time").define(ConfigName.TIME_ZONE,ConfigDef.Type.STRING,ConfigName.TIME_ZONE_DEFAULT_VALUE, ConfigDef.Importance.HIGH,"Time Zone Field").define(ConfigName.PATTERN_FORMAT_FIELD_NAME,ConfigDef.Type.STRING,ConfigName.PATTERN_FORMAT_DEFAULT_VALUE,ConfigDef.Importance.HIGH,"Date Pattern");
    public static final String PURPOSE = "Adding update time from kafka to database";
    private String fieldName;
    private String timezone;
    private String pattern;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.UPD_TIME_FIELD_NAME);
        timezone = config.getString(ConfigName.TIME_ZONE);
        pattern = config.getString(ConfigName.PATTERN_FORMAT_FIELD_NAME);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        updatedValue.put(fieldName, getCurrentTime());

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(fieldName, getCurrentTime());

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private String getCurrentTime() {
        Instant instant = Instant.now();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern).withZone(ZoneId.of(timezone));
        String formatInstant = dateTimeFormatter.format(instant);
        return formatInstant;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(fieldName, Schema.STRING_SCHEMA);

        return builder.build();
    }

    public static class Key<R extends ConnectRecord<R>> extends UpdateTime<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends UpdateTime<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
