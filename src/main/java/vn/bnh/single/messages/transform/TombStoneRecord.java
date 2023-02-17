package vn.bnh.single.messages.transform;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class TombStoneRecord<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger logger = LoggerFactory.getLogger(UpdateTime.class);
    private static final String OVERVIEW_DOC = "transform messages to tombstone messages";

    private interface ConfigName {
        String FIELD_NAME = "Field Name";
        String DEFAULT_FIELD_NAME = "op_type";
        String OPERATION = "operation";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, ConfigName.DEFAULT_FIELD_NAME, ConfigDef.Importance.HIGH, "field for op type").define(ConfigName.OPERATION,ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"operation");
    public static String PURPOSE = "transform message with op_type delete to tombstone messages";
    String fieldName;
    String operation;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        operation = config.getString(ConfigName.OPERATION);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        Map<String, Object> updatedValue = new HashMap<>();
        if(value.get(fieldName)==operation){
            updatedValue.put(null,null);
            return newRecord(record, null, updatedValue);
        }else {

            return newRecord(record, null, record);
        }
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        Struct updatedValue = new Struct(updatedSchema);
//        for (Field field : value.schema().fields()) {
//            updatedValue.put(field.name(), value.get(field));
//        }
//        updatedValue.put(fieldName, createTombstoneMessage());
//        return newRecord(record, updatedSchema, updatedValue);
        if(value.get(fieldName)==operation){
            updatedValue.put("null",null);
            return newRecord(record, null, updatedValue);
        }else {

            return newRecord(record, null, record);
        }
    }

    private String createTombstoneMessage(){

        return null;
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

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }
    public static class Key<R extends ConnectRecord<R>> extends TombStoneRecord<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends TombStoneRecord<R> {

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
