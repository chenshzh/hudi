/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.testutils.HiveTestService.HS2_JDBC_URL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_ASSUME_DATE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

public class HoodieDeltaStreamerTestBase extends UtilitiesTestBase {

  static final Random RANDOM = new Random();
  static final String PROPS_FILENAME_TEST_SOURCE = "test-source.properties";
  static final String PROPS_FILENAME_TEST_SOURCE1 = "test-source1.properties";
  static final String PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1 = "test-invalid-hive-sync-source1.properties";
  static final String PROPS_INVALID_FILE = "test-invalid-props.properties";
  static final String PROPS_INVALID_TABLE_CONFIG_FILE = "test-invalid-table-config.properties";
  static final String PROPS_FILENAME_TEST_INVALID = "test-invalid.properties";
  static final String PROPS_FILENAME_INFER_COMPLEX_KEYGEN = "test-infer-complex-keygen.properties";
  static final String PROPS_FILENAME_INFER_NONPARTITIONED_KEYGEN = "test-infer-nonpartitioned-keygen.properties";
  static final String PROPS_FILENAME_TEST_CSV = "test-csv-dfs-source.properties";
  static final String PROPS_FILENAME_TEST_PARQUET = "test-parquet-dfs-source.properties";
  static final String PROPS_FILENAME_TEST_ORC = "test-orc-dfs-source.properties";
  static final String PROPS_FILENAME_TEST_JSON_KAFKA = "test-json-kafka-dfs-source.properties";
  static final String PROPS_FILENAME_TEST_SQL_SOURCE = "test-sql-source-source.properties";
  static final String PROPS_FILENAME_TEST_MULTI_WRITER = "test-multi-writer.properties";
  static final String FIRST_PARQUET_FILE_NAME = "1.parquet";
  static final String FIRST_ORC_FILE_NAME = "1.orc";
  static String PARQUET_SOURCE_ROOT;
  static String ORC_SOURCE_ROOT;
  static String JSON_KAFKA_SOURCE_ROOT;
  static final int PARQUET_NUM_RECORDS = 5;
  static final int ORC_NUM_RECORDS = 5;
  static final int CSV_NUM_RECORDS = 3;
  static final int JSON_KAFKA_NUM_RECORDS = 5;
  static final int SQL_SOURCE_NUM_RECORDS = 1000;
  String kafkaCheckpointType = "string";
  // Required fields
  static final String TGT_BASE_PATH_PARAM = "--target-base-path";
  static final String TGT_BASE_PATH_VALUE = "s3://mybucket/blah";
  static final String TABLE_TYPE_PARAM = "--table-type";
  static final String TABLE_TYPE_VALUE = "COPY_ON_WRITE";
  static final String TARGET_TABLE_PARAM = "--target-table";
  static final String TARGET_TABLE_VALUE = "test";
  static final String BASE_FILE_FORMAT_PARAM = "--base-file-format";
  static final String BASE_FILE_FORMAT_VALUE = "PARQUET";
  static final String SOURCE_LIMIT_PARAM = "--source-limit";
  static final String SOURCE_LIMIT_VALUE = "500";
  static final String ENABLE_HIVE_SYNC_PARAM = "--enable-hive-sync";
  static final String HOODIE_CONF_PARAM = "--hoodie-conf";
  static final String HOODIE_CONF_VALUE1 = "hoodie.datasource.hive_sync.table=test_table";
  static final String HOODIE_CONF_VALUE2 = "hoodie.datasource.write.recordkey.field=Field1,Field2,Field3";
  public static KafkaTestUtils testUtils;
  protected static String topicName;
  protected static String defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
  protected static int testNum = 1;

  protected static void prepareTestSetup() throws IOException {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFiles";
    ORC_SOURCE_ROOT = basePath + "/orcFiles";
    JSON_KAFKA_SOURCE_ROOT = basePath + "/jsonKafkaFiles";
    testUtils = new KafkaTestUtils();
    testUtils.setup();
    topicName = "topic" + testNum;
    prepareInitialConfigs(fs, basePath, testUtils.brokerAddress());
    prepareParquetDFSFiles(PARQUET_NUM_RECORDS, PARQUET_SOURCE_ROOT);
    prepareORCDFSFiles(ORC_NUM_RECORDS, ORC_SOURCE_ROOT);
  }

  protected static void prepareInitialConfigs(FileSystem dfs, String dfsBasePath, String brokerAddress) throws IOException {
    // prepare the configs.
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/base.properties", dfs, dfsBasePath + "/config/base.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/sql-transformer.properties", dfs,
        dfsBasePath + "/sql-transformer.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source.avsc", dfs, dfsBasePath + "/source.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_evolved.avsc", dfs, dfsBasePath + "/source_evolved.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_evolved_post_processed.avsc", dfs, dfsBasePath + "/source_evolved_post_processed.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source-flattened.avsc", dfs, dfsBasePath + "/source-flattened.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target.avsc", dfs, dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target-flattened.avsc", dfs, dfsBasePath + "/target-flattened.avsc");

    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_short_trip_uber.avsc", dfs, dfsBasePath + "/source_short_trip_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/source_uber.avsc", dfs, dfsBasePath + "/source_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target_short_trip_uber.avsc", dfs, dfsBasePath + "/target_short_trip_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/target_uber.avsc", dfs, dfsBasePath + "/target_uber.avsc");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/invalid_hive_sync_uber_config.properties", dfs, dfsBasePath + "/config/invalid_hive_sync_uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/uber_config.properties", dfs, dfsBasePath + "/config/uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/short_trip_uber_config.properties", dfs, dfsBasePath + "/config/short_trip_uber_config.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/clusteringjob.properties", dfs, dfsBasePath + "/clusteringjob.properties");
    UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/indexer.properties", dfs, dfsBasePath + "/indexer.properties");

    writeCommonPropsToFile(dfs, dfsBasePath);

    // Properties used for the delta-streamer which incrementally pulls from upstream Hudi source table and writes to
    // downstream hudi table
    TypedProperties downstreamProps = new TypedProperties();
    downstreamProps.setProperty("include", "base.properties");
    downstreamProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    downstreamProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");

    // Source schema is the target schema of upstream table
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/target.avsc");
    downstreamProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(downstreamProps, dfs, dfsBasePath + "/test-downstream-source.properties");

    // Properties used for testing invalid key generator
    TypedProperties invalidProps = new TypedProperties();
    invalidProps.setProperty("include", "sql-transformer.properties");
    invalidProps.setProperty("hoodie.datasource.write.keygenerator.class", "invalid");
    invalidProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    invalidProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    invalidProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    invalidProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(invalidProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_INVALID);

    // Properties used for testing inferring key generator for complex key generator
    TypedProperties inferKeygenProps = new TypedProperties();
    inferKeygenProps.setProperty("include", "base.properties");
    inferKeygenProps.setProperty("hoodie.datasource.write.recordkey.field", "timestamp,_row_key");
    inferKeygenProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    inferKeygenProps.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    inferKeygenProps.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");
    UtilitiesTestBase.Helpers.savePropsToDFS(inferKeygenProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_INFER_COMPLEX_KEYGEN);

    // Properties used for testing inferring key generator for non-partitioned key generator
    inferKeygenProps.setProperty("hoodie.datasource.write.partitionpath.field", "");
    UtilitiesTestBase.Helpers.savePropsToDFS(inferKeygenProps, dfs, dfsBasePath + "/" + PROPS_FILENAME_INFER_NONPARTITIONED_KEYGEN);

    TypedProperties props1 = new TypedProperties();
    populateAllCommonProps(props1, dfsBasePath, brokerAddress);
    UtilitiesTestBase.Helpers.savePropsToDFS(props1, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE1);

    TypedProperties properties = new TypedProperties();
    populateInvalidTableConfigFilePathProps(properties, dfsBasePath);
    UtilitiesTestBase.Helpers.savePropsToDFS(properties, dfs, dfsBasePath + "/" + PROPS_INVALID_TABLE_CONFIG_FILE);

    TypedProperties invalidHiveSyncProps = new TypedProperties();
    invalidHiveSyncProps.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "uber_db.dummy_table_uber");
    invalidHiveSyncProps.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/invalid_hive_sync_uber_config.properties");
    UtilitiesTestBase.Helpers.savePropsToDFS(invalidHiveSyncProps, dfs, dfsBasePath + "/" + PROPS_INVALID_HIVE_SYNC_TEST_SOURCE1);
  }

  protected static void writeCommonPropsToFile(FileSystem dfs, String dfsBasePath) throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty("include", "sql-transformer.properties");
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/source.avsc");
    props.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.file", dfsBasePath + "/target.avsc");

    // Hive Configs
    props.setProperty(HIVE_URL.key(), HS2_JDBC_URL);
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "testdb1");
    props.setProperty(META_SYNC_TABLE_NAME.key(), "hive_trips");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        MultiPartKeysValueExtractor.class.getName());
    UtilitiesTestBase.Helpers.savePropsToDFS(props, dfs, dfsBasePath + "/" + PROPS_FILENAME_TEST_SOURCE);
  }

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, true, false);
    prepareTestSetup();
  }

  @AfterAll
  public static void cleanupKafkaTestUtils() {
    if (testUtils != null) {
      testUtils.teardown();
    }
  }

  @BeforeEach
  public void resetTestDataSource() {
    TestDataSource.returnEmptyBatch = false;
  }

  protected static void populateInvalidTableConfigFilePathProps(TypedProperties props, String dfsBasePath) {
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
    props.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "uber_db.dummy_table_uber");
    props.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/invalid_uber_config.properties");
  }

  protected static void populateAllCommonProps(TypedProperties props, String dfsBasePath, String brokerAddress) {
    populateCommonProps(props, dfsBasePath);
    populateCommonKafkaProps(props, brokerAddress);
    populateCommonHiveProps(props);
  }

  protected static void populateCommonProps(TypedProperties props, String dfsBasePath) {
    props.setProperty("hoodie.datasource.write.keygenerator.class", TestHoodieDeltaStreamer.TestGenerator.class.getName());
    props.setProperty("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyyMMdd");
    props.setProperty("hoodie.deltastreamer.ingestion.tablesToBeIngested", "short_trip_db.dummy_table_short_trip,uber_db.dummy_table_uber");
    props.setProperty("hoodie.deltastreamer.ingestion.uber_db.dummy_table_uber.configFile", dfsBasePath + "/config/uber_config.properties");
    props.setProperty("hoodie.deltastreamer.ingestion.short_trip_db.dummy_table_short_trip.configFile", dfsBasePath + "/config/short_trip_uber_config.properties");
  }

  protected static void populateCommonKafkaProps(TypedProperties props, String brokerAddress) {
    //Kafka source properties
    props.setProperty("bootstrap.servers", brokerAddress);
    props.setProperty("auto.offset.reset", "earliest");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", String.valueOf(5000));
  }

  protected static void populateCommonHiveProps(TypedProperties props) {
    // Hive Configs
    props.setProperty(HIVE_URL.key(), HS2_JDBC_URL);
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "testdb2");
    props.setProperty(META_SYNC_ASSUME_DATE_PARTITION.key(), "false");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        MultiPartKeysValueExtractor.class.getName());
  }

  protected static void prepareParquetDFSFiles(int numRecords) throws IOException {
    prepareParquetDFSFiles(numRecords, PARQUET_SOURCE_ROOT);
  }

  protected static void prepareParquetDFSFiles(int numRecords, String baseParquetPath) throws IOException {
    prepareParquetDFSFiles(numRecords, baseParquetPath, FIRST_PARQUET_FILE_NAME, false, null, null);
  }

  protected static HoodieTestDataGenerator prepareParquetDFSFiles(int numRecords, String baseParquetPath, String fileName, boolean useCustomSchema,
                                                                        String schemaStr, Schema schema) throws IOException {
    String path = baseParquetPath + "/" + fileName;
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    if (useCustomSchema) {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInsertsAsPerSchema("000", numRecords, schemaStr),
          schema), new Path(path), HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    } else {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInserts("000", numRecords)), new Path(path));
    }
    return dataGenerator;
  }

  protected static void prepareParquetDFSUpdates(int numRecords, String baseParquetPath, String fileName, boolean useCustomSchema,
                                                                  String schemaStr, Schema schema, HoodieTestDataGenerator dataGenerator, String timestamp) throws IOException {
    String path = baseParquetPath + "/" + fileName;
    if (useCustomSchema) {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateUpdatesAsPerSchema(timestamp, numRecords, schemaStr),
          schema), new Path(path), HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    } else {
      Helpers.saveParquetToDFS(Helpers.toGenericRecords(
          dataGenerator.generateUpdates(timestamp, numRecords)), new Path(path));
    }
  }

  protected static void prepareORCDFSFiles(int numRecords) throws IOException {
    prepareORCDFSFiles(numRecords, ORC_SOURCE_ROOT);
  }

  protected static void prepareORCDFSFiles(int numRecords, String baseORCPath) throws IOException {
    prepareORCDFSFiles(numRecords, baseORCPath, FIRST_ORC_FILE_NAME, false, null, null);
  }

  protected static void prepareORCDFSFiles(int numRecords, String baseORCPath, String fileName, boolean useCustomSchema,
                                               String schemaStr, Schema schema) throws IOException {
    String path = baseORCPath + "/" + fileName;
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    if (useCustomSchema) {
      Helpers.saveORCToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInsertsAsPerSchema("000", numRecords, schemaStr),
          schema), new Path(path), HoodieTestDataGenerator.ORC_TRIP_SCHEMA);
    } else {
      Helpers.saveORCToDFS(Helpers.toGenericRecords(
          dataGenerator.generateInserts("000", numRecords)), new Path(path));
    }
  }

  static void addCommitToTimeline(HoodieTableMetaClient metaClient) throws IOException {
    addCommitToTimeline(metaClient, Collections.emptyMap());
  }

  static void addCommitToTimeline(HoodieTableMetaClient metaClient, Map<String, String> extraMetadata) throws IOException {
    addCommitToTimeline(metaClient, WriteOperationType.UPSERT, HoodieTimeline.COMMIT_ACTION, extraMetadata);
  }

  static void addReplaceCommitToTimeline(HoodieTableMetaClient metaClient, Map<String, String> extraMetadata) throws IOException {
    addCommitToTimeline(metaClient, WriteOperationType.CLUSTER, HoodieTimeline.REPLACE_COMMIT_ACTION, extraMetadata);
  }

  static void addCommitToTimeline(HoodieTableMetaClient metaClient, WriteOperationType writeOperationType, String commitActiontype,
                                  Map<String, String> extraMetadata) throws IOException {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(writeOperationType);
    extraMetadata.forEach((k, v) -> commitMetadata.getExtraMetadata().put(k, v));
    String commitTime = HoodieActiveTimeline.createNewInstantTime();
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, commitActiontype, commitTime));
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.INFLIGHT, commitActiontype, commitTime));
    metaClient.getActiveTimeline().saveAsComplete(
        new HoodieInstant(HoodieInstant.State.INFLIGHT, commitActiontype, commitTime),
        Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
  }

}
