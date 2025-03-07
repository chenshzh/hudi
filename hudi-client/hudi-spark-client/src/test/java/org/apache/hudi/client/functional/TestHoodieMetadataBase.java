/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client.functional;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.HoodieTimelineArchiver;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataKeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ASYNC_CLEAN;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.model.WriteOperationType.INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;

public class TestHoodieMetadataBase extends HoodieClientTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieMetadataBase.class);

  protected static HoodieTestTable testTable;
  protected String metadataTableBasePath;
  protected HoodieTableType tableType;
  protected HoodieWriteConfig writeConfig;
  protected HoodieTableMetadataWriter metadataWriter;

  public void init(HoodieTableType tableType) throws IOException {
    init(tableType, true);
  }

  public void init(HoodieTableType tableType, HoodieWriteConfig writeConfig) throws IOException {
    init(tableType, Option.of(writeConfig), true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable) throws IOException {
    init(tableType, enableMetadataTable, true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable, boolean enableColumnStats) throws IOException {
    init(tableType, enableMetadataTable, true, false, false);
  }

  public void init(HoodieTableType tableType, boolean enableMetadataTable, boolean enableFullScan, boolean enableMetrics, boolean
      validateMetadataPayloadStateConsistency) throws IOException {
    init(tableType, Option.empty(), enableMetadataTable, enableMetrics,
        validateMetadataPayloadStateConsistency);
  }

  public void init(HoodieTableType tableType, Option<HoodieWriteConfig> writeConfig, boolean enableMetadataTable,
                   boolean enableMetrics, boolean validateMetadataPayloadStateConsistency) throws IOException {
    this.tableType = tableType;
    initPath();
    initSparkContexts("TestHoodieMetadata");
    initFileSystem();
    fs.mkdirs(new Path(basePath));
    initTimelineService();
    initMetaClient(tableType);
    initTestDataGenerator();
    metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    this.writeConfig = writeConfig.isPresent()
        ? writeConfig.get() : getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true,
        enableMetadataTable, enableMetrics, true,
        validateMetadataPayloadStateConsistency)
        .build();
    initWriteConfigAndMetatableWriter(this.writeConfig, enableMetadataTable);
  }

  protected void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) {
    this.writeConfig = writeConfig;
    if (enableMetadataTable) {
      metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, writeConfig, context);
      testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);
    } else {
      testTable = HoodieTestTable.of(metaClient);
    }
  }

  @AfterEach
  public void clean() throws Exception {
    cleanupResources();
  }

  protected void doWriteInsertAndUpsert(HoodieTestTable testTable, String commit1, String commit2, boolean nonPartitioned) throws Exception {
    testTable.doWriteOperation(commit1, INSERT, nonPartitioned ? asList("") : asList("p1", "p2"), nonPartitioned ? asList("") : asList("p1", "p2"),
        4, false);
    testTable.doWriteOperation(commit2, UPSERT, nonPartitioned ? asList("") : asList("p1", "p2"),
        4, false);
    validateMetadata(testTable);
  }

  protected void doWriteOperationAndValidateMetadata(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperation(testTable, commitTime);
    validateMetadata(testTable);
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperation(testTable, commitTime, UPSERT);
  }

  protected void doWriteOperationAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doWriteOperationAndValidate(testTable, commitTime, UPSERT);
  }

  protected void doWriteOperationAndValidate(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    doWriteOperation(testTable, commitTime, operationType);
    validateMetadata(testTable);
  }

  protected void doWriteOperationNonPartitioned(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    testTable.doWriteOperation(commitTime, operationType, emptyList(), asList(""), 3);
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType, boolean nonPartitioned) throws Exception {
    if (nonPartitioned) {
      doWriteOperationNonPartitioned(testTable, commitTime, operationType);
    } else {
      doWriteOperation(testTable, commitTime, operationType);
    }
  }

  protected void doWriteOperation(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    testTable.doWriteOperation(commitTime, operationType, emptyList(), asList("p1", "p2"), 3);
  }

  protected HoodieCommitMetadata doWriteOperationWithMeta(HoodieTestTable testTable, String commitTime, WriteOperationType operationType) throws Exception {
    return testTable.doWriteOperation(commitTime, operationType, emptyList(), asList("p1", "p2"), 3);
  }

  protected void doClean(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws IOException {
    doCleanInternal(testTable, commitTime, commitsToClean, false);
  }

  protected void doCleanAndValidate(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws IOException {
    doCleanInternal(testTable, commitTime, commitsToClean, true);
  }

  private void doCleanInternal(HoodieTestTable testTable, String commitTime, List<String> commitsToClean, boolean validate) throws IOException {
    testTable.doCleanBasedOnCommits(commitTime, commitsToClean);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doCompactionNonPartitioned(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, false, true);
  }

  protected void doCompaction(HoodieTestTable testTable, String commitTime, boolean nonPartitioned) throws Exception {
    doCompactionInternal(testTable, commitTime, false, nonPartitioned);
  }

  protected void doCompaction(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, false, false);
  }

  protected void doCompactionNonPartitionedAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, true, true);
  }

  protected void doCompactionAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doCompactionInternal(testTable, commitTime, true, false);
  }

  private void doCompactionInternal(HoodieTestTable testTable, String commitTime, boolean validate, boolean nonPartitioned) throws Exception {
    testTable.doCompaction(commitTime, nonPartitioned ? asList("") : asList("p1", "p2"));
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doCluster(HoodieTestTable testTable, String commitTime) throws Exception {
    doClusterInternal(testTable, commitTime, false);
  }

  protected void doClusterAndValidate(HoodieTestTable testTable, String commitTime) throws Exception {
    doClusterInternal(testTable, commitTime, true);
  }

  protected void doClusterInternal(HoodieTestTable testTable, String commitTime, boolean validate) throws Exception {
    testTable.doCluster(commitTime, new HashMap<>(), Arrays.asList("p1", "p2"), 2);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doRollback(HoodieTestTable testTable, String commitToRollback, String rollbackTime) throws Exception {
    doRollbackInternal(testTable, commitToRollback, rollbackTime, false);
  }

  protected void doRollbackAndValidate(HoodieTestTable testTable, String commitToRollback, String rollbackTime) throws Exception {
    doRollbackInternal(testTable, commitToRollback, rollbackTime, true);
  }

  private void doRollbackInternal(HoodieTestTable testTable, String commitToRollback, String rollbackTime, boolean validate) throws Exception {
    testTable.doRollback(commitToRollback, rollbackTime);
    if (validate) {
      validateMetadata(testTable);
    }
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, String commitTime) throws Exception {
    doPreBootstrapWriteOperation(testTable, UPSERT, commitTime);
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, WriteOperationType writeOperationType, String commitTime) throws Exception {
    doPreBootstrapWriteOperation(testTable, writeOperationType, commitTime, 2);
  }

  protected void doPreBootstrapWriteOperation(HoodieTestTable testTable, WriteOperationType writeOperationType, String commitTime, int filesPerPartition) throws Exception {
    testTable.doWriteOperation(commitTime, writeOperationType, asList("p1", "p2"), asList("p1", "p2"),
        filesPerPartition, true);
  }

  protected void doPreBootstrapClean(HoodieTestTable testTable, String commitTime, List<String> commitsToClean) throws Exception {
    testTable.doCleanBasedOnCommits(commitTime, commitsToClean);
  }

  protected void doPreBootstrapRollback(HoodieTestTable testTable, String rollbackTime, String commitToRollback) throws Exception {
    testTable.doRollback(commitToRollback, rollbackTime);
  }

  protected void doPrebootstrapCompaction(HoodieTestTable testTable, String commitTime) throws Exception {
    doPrebootstrapCompaction(testTable, commitTime, Arrays.asList("p1", "p2"));
  }

  protected void doPrebootstrapCompaction(HoodieTestTable testTable, String commitTime, List<String> partitions) throws Exception {
    testTable.doCompaction(commitTime, partitions);
  }

  protected void doPreBootstrapCluster(HoodieTestTable testTable, String commitTime) throws Exception {
    testTable.doCluster(commitTime, new HashMap<>(), Arrays.asList("p1", "p2"), 2);
  }

  protected void doPreBootstrapRestore(HoodieTestTable testTable, String restoreTime, String commitToRestore) throws Exception {
    testTable.doRestore(commitToRestore, restoreTime);
  }

  protected void archiveDataTable(HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) throws IOException {
    HoodieTable table = HoodieSparkTable.create(writeConfig, context, metaClient);
    HoodieTimelineArchiver archiver = new HoodieTimelineArchiver(writeConfig, table);
    archiver.archiveIfRequired(context);
  }

  protected void validateMetadata(HoodieTestTable testTable) throws IOException {
    validateMetadata(testTable, emptyList());
  }

  protected void validateMetadata(HoodieTestTable testTable, boolean doFullValidation) throws IOException {
    validateMetadata(testTable, emptyList(), doFullValidation);
  }

  protected void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits) throws IOException {
    validateMetadata(testTable, inflightCommits, false);
  }

  protected void validateMetadata(HoodieTestTable testTable, List<String> inflightCommits, boolean doFullValidation) throws IOException {
    validateMetadata(testTable, inflightCommits, writeConfig, metadataTableBasePath, doFullValidation);
  }

  protected HoodieWriteConfig getWriteConfig(boolean autoCommit, boolean useFileListingMetadata) {
    return getWriteConfigBuilder(autoCommit, useFileListingMetadata, false).build();
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(boolean autoCommit, boolean useFileListingMetadata, boolean enableMetrics) {
    return getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, autoCommit, useFileListingMetadata, enableMetrics);
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata,
                                                            boolean enableMetrics) {
    return getWriteConfigBuilder(policy, autoCommit, useFileListingMetadata, enableMetrics, true, false);
  }

  protected HoodieWriteConfig.Builder getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy policy, boolean autoCommit, boolean useFileListingMetadata,
                                                            boolean enableMetrics, boolean useRollbackUsingMarkers,
                                                            boolean validateMetadataPayloadConsistency) {
    Properties properties = new Properties();
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2).withDeleteParallelism(2).withRollbackParallelism(2).withFinalizeWriteParallelism(2)
        .withAutoCommit(autoCommit)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024 * 1024)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withFailedWritesCleaningPolicy(policy)
            .withAutoClean(false).retainCommits(1).retainFileVersions(1)
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true).forTable("test-trip-table")
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(useFileListingMetadata)
            .enableMetrics(enableMetrics)
            .ignoreSpuriousDeletes(validateMetadataPayloadConsistency)
            .build())
        .withMetricsConfig(HoodieMetricsConfig.newBuilder().on(enableMetrics)
            .withExecutorMetrics(true).build())
        .withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
            .usePrefix("unit-test").build())
        .withRollbackUsingMarkers(useRollbackUsingMarkers)
        .withProperties(properties);
  }

  /**
   * Fetching WriteConfig for metadata table from Data table's writeConfig is not trivial and
   * the method is not public in source code. so, for now, using this method which mimics source code.
   */
  protected HoodieWriteConfig getMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getMetadataInsertParallelism();

    int minCommitsToKeep = Math.max(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMinCommitsToKeep());
    int maxCommitsToKeep = Math.max(writeConfig.getMetadataMaxCommitsToKeep(), writeConfig.getMaxCommitsToKeep());

    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.SINGLE_WRITER)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).withFileListingParallelism(writeConfig.getFileListingParallelism()).build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withRollbackUsingMarkers(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX)
        // we will trigger cleaning manually, to control the instant times
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAsyncClean(DEFAULT_METADATA_ASYNC_CLEAN)
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
            .retainCommits(DEFAULT_METADATA_CLEANER_COMMITS_RETAINED)
            .build())
        // we will trigger archival manually, to control the instant times
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(minCommitsToKeep, maxCommitsToKeep).build())
        // we will trigger compaction manually, to control the instant times
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism)
        .withAllowMultiWriteOnSameInstant(true)
        .withKeyGenerator(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .withPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS);

    // RecordKey properties are needed for the metadata table records
    final Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), HoodieMetadataPayload.KEY_FIELD_NAME);
    properties.put("hoodie.datasource.write.recordkey.field", HoodieMetadataPayload.KEY_FIELD_NAME);
    builder.withProperties(properties);

    if (writeConfig.isMetricsOn()) {
      builder.withMetricsConfig(HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .on(true).build());
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          builder.withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
              .onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix()).build());
          break;
        case JMX:
          builder.withMetricsJmxConfig(HoodieMetricsJmxConfig.newBuilder()
              .onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost())
              .build());
          break;
        case DATADOG:
        case PROMETHEUS:
        case PROMETHEUS_PUSHGATEWAY:
        case CONSOLE:
        case INMEMORY:
        case CLOUDWATCH:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }
    }
    return builder.build();
  }
}
