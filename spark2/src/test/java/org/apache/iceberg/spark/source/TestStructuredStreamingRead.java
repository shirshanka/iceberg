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

package org.apache.iceberg.spark.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.*;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestStructuredStreamingRead {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      required(3, "ts", Types.TimestampType.withZone())
  );
  private static SparkSession spark = null;
  private static Path parent = null;
  private static File tableLocation = null;
  private static Table table = null;

  private static TestHiveMetastore metastore;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() throws Exception {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf conf = metastore.hiveConf();

    String metastoreURI = conf.get(HiveConf.ConfVars.METASTOREURIS.varname);
    // Create a spark session.
    TestStructuredStreamingRead.spark = SparkSession.builder()
            .enableHiveSupport()
            .config("spark.hadoop.hive.metastore.uris", metastoreURI)
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate();

    parent = Files.createTempDirectory("test");

    HiveCatalog tables = HiveCatalogs.loadCatalog(conf);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    table = tables.createTable(TableIdentifier.of("default", "t"), SCHEMA, spec);

    List<List<SimpleRecord2>> expected = Lists.newArrayList(
        Lists.newArrayList(new SimpleRecord2(1, "1", 45L)),
        Lists.newArrayList(new SimpleRecord2(2, "2", 100L)),
        Lists.newArrayList(new SimpleRecord2(3, "3", 150L)),
        Lists.newArrayList(new SimpleRecord2(4, "4", 200L))
    );

    // Write records one by one to generate 4 snapshots.
    for (List<SimpleRecord2> l : expected) {
      Dataset<Row> df = spark.createDataFrame(l, SimpleRecord2.class);
      df.select("id", "data")
        .withColumn("ts", functions.current_timestamp())
        .write()
        .format("iceberg")
        .mode("append")
        .save("default.t");
    }
    table.refresh();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead.spark;
    TestStructuredStreamingRead.spark = null;
    currentSpark.stop();
  }

  @Test
  public void sparkDataStream() throws StreamingQueryException {
    ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    AtomicReference<Integer> i = new AtomicReference<>(0);
    executorService.scheduleWithFixedDelay(
            () -> {
              System.out.println("Writing records in the table");
              List<Record> records = RandomGenericData.generate(SCHEMA, 10, 0L);
              records.forEach(r -> r.set(1, i.toString()));
              DataFile dataFile = writeFile(TestHelpers.Row.of(i.toString()), records);
              table.newAppend().appendFile(dataFile).commit();
              i.updateAndGet(v -> v + 1);
            },
            0,
            1,
            TimeUnit.MINUTES
    );
    Dataset<Row> df = spark.readStream().format("iceberg").load("default.t");
    Dataset<Row> id = df
            .withWatermark("ts", "1 minutes")
            //.groupBy(functions.window(df.col("ts"), "1 minutes"))
            .groupBy("ts")
            .count();
    id.writeStream()
            .format("console")
            .trigger(Trigger.Once())
            .start()
            .awaitTermination();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFromStart() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    // Getting all appends from initial snapshot.
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 0, true, false), Long.MAX_VALUE);
    Assert.assertEquals("Batches with unlimited size control should have 4 snapshots", 4, pendingBatches.size());

    List<Long> batchSnapshotIds = pendingBatches.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertEquals("Snapshot id of each batch should match snapshot id of table", snapshotIds, batchSnapshotIds);

    // Getting appends from initial snapshot with last index, 1st snapshot should be an empty batch.
    List<MicroBatch> pendingBatches1 = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 1, true, false), Long.MAX_VALUE);

    Assert.assertEquals("Batches with unlimited size control from initial id should have 4 snapshots",
        4, pendingBatches1.size());
    MicroBatch batch = pendingBatches1.get(0);
    // 1st batch should be empty, since the starting offset is the end of this snapshot.
    Assert.assertEquals("1st batch should be empty, have 0 size batch", 0L, batch.sizeInBytes());
    Assert.assertEquals("1st batch should be empty, endFileIndex should be equal to start", 1, batch.endFileIndex());
    Assert.assertTrue("1st batch should be empty", Iterables.isEmpty(batch.tasks()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFrom2ndSnapshot() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    // Getting appends from 2nd snapshot, 1st snapshot should be filtered out.
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(1), 0, false, false), Long.MAX_VALUE);

    Assert.assertEquals(3, pendingBatches.size());
    List<Long> batchSnapshotIds = pendingBatches.stream()
        .map(MicroBatch::snapshotId)
        .collect(Collectors.toList());
    Assert.assertFalse("1st snapshot should be filtered", batchSnapshotIds.contains(initialSnapshotId));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesFromLastSnapshot() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    IcebergSource source = new IcebergSource();

    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Getting appends from last snapshot with last index, should get an empty batch.
    long lastSnapshotId = snapshotIds.get(3);
    List<MicroBatch> pendingBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(lastSnapshotId, 1, false, false), Long.MAX_VALUE);

    Assert.assertEquals("Should only have 1 batch with last snapshot", 1, pendingBatches.size());
    MicroBatch batch = pendingBatches.get(0);
    Assert.assertEquals("batch should have 0 size", 0L, batch.sizeInBytes());
    Assert.assertEquals("batch endFileIndex should be euqal to start", 1, batch.endFileIndex());
    Assert.assertTrue("batch should be empty", Iterables.isEmpty(batch.tasks()));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit1000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);
    long initialSnapshotId = snapshotIds.get(0);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // The size of each data file is around 600 bytes.
    // Max size set to 1000. One additional batch will be added because the left size is less than file size,
    // MicroBatchBuilder will add one more to avoid stuck.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(initialSnapshotId, 0, true, false), 1000);

    Assert.assertEquals("Should have 2 batches", 2L, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("1st batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertTrue("1st batch should be the last index of 1st snapshot", batch.lastIndexOfSnapshot());
    Assert.assertEquals("1st batch should only have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("1st batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);

    MicroBatch batch1 = rateLimitedBatches.get(1);
    Assert.assertEquals("2nd batch's endFileIndex should reach to the end of file indexes", 1, batch1.endFileIndex());
    Assert.assertTrue("2nd batch should be the last of 2nd snapshot", batch1.lastIndexOfSnapshot());
    Assert.assertEquals("2nd batch should only have 1 task", 1, batch1.tasks().size());
    Assert.assertTrue("2nd batch's size should be aound 600", batch1.sizeInBytes() < 1000 && batch1.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit100() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // Max size less than file size, should have one batch added to avoid stuck.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(1), 1, false, true), 100);

    Assert.assertEquals("Should only have 1 batch", 1, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("Batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertTrue("Batch should be the last of 1st snapshot", batch.lastIndexOfSnapshot());
    Assert.assertEquals("Batch should have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("Batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetChangesWithRateLimit10000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    // Max size set to 10000, the last left batch will be added.
    List<MicroBatch> rateLimitedBatches = streamingReader.getChangesWithRateLimit(
        new StreamingOffset(snapshotIds.get(2), 1, false, true), 10000);

    Assert.assertEquals("Should only have 1 batch", 1, rateLimitedBatches.size());
    MicroBatch batch = rateLimitedBatches.get(0);
    Assert.assertEquals("Batch's endFileIndex should reach to the end of file indexes", 1, batch.endFileIndex());
    Assert.assertEquals("Batch should have 1 task", 1, batch.tasks().size());
    Assert.assertTrue("Batch should have 1 task", batch.lastIndexOfSnapshot());
    Assert.assertTrue("Batch's size should be around 600", batch.sizeInBytes() < 1000 && batch.sizeInBytes() > 0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithDefaultRateLimit() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Default max size per batch, this will consume all the data of this table.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString()));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);
    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());

    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals("Start offset's snapshot id should be 1st snapshot id",
        snapshotIds.get(0).longValue(), start.snapshotId());
    Assert.assertEquals("Start offset's index should be the start index of 1st snapshot", 0, start.index());
    Assert.assertTrue("Start offset's snapshot id should be a starting snapshot id", start.isStartingSnapshotId());
    Assert.assertFalse("Start offset should not be the last index of 1st snapshot", start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot should be the last snapshot id",
        snapshotIds.get(3).longValue(), end.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end.index());
    Assert.assertFalse("End offset's snapshot id should not a starting snapshot id", end.isStartingSnapshotId());
    Assert.assertTrue("End offset should be the last index of 3rd snapshot", end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset should be same to start offset since there's no more batches to consume",
        end1, end);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithRateLimit1000() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Max size to 1000, this will generate two MicroBatches per consuming.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals("Start offset's snapshot id should be 1st snapshot id",
        snapshotIds.get(0).longValue(), start.snapshotId());
    Assert.assertEquals("Start offset's index should be the start index of 1st snapshot", 0, start.index());
    Assert.assertTrue("Start offset's snapshot id should be a starting snapshot id", start.isStartingSnapshotId());
    Assert.assertFalse("Start offset should not be the last index of 1st snapshot", start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot should be the 2nd snapshot id",
        snapshotIds.get(1).longValue(), end.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end.index());
    Assert.assertFalse("End offset's snapshot id should not a starting snapshot id", end.isStartingSnapshotId());
    Assert.assertTrue("End offset should be the last index of 2nd snapshot", end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be last snapshot id",
        snapshotIds.get(3).longValue(), end1.snapshotId());
    Assert.assertEquals("End offset should be the last index of last snapshot", 1, end1.index());
    Assert.assertFalse("End offset's snapshot id should not a starting snapshot id", end1.isStartingSnapshotId());
    Assert.assertTrue("End offset should be the last index of last snapshot", end1.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset should be same to start offset since there's no more batches to consume",
        end2, end1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetOffsetWithRateLimit100() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // Max size to 100, will generate 1 MicroBatch per consuming.
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "max-size-per-batch", "100"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals("Start offset's snapshot id should be 1st snapshot id",
        snapshotIds.get(0).longValue(), start.snapshotId());
    Assert.assertEquals("Start offset's index should be the start index of 1st snapshot", 0, start.index());
    Assert.assertTrue("Start offset's snapshot id should be a starting snapshot id", start.isStartingSnapshotId());
    Assert.assertFalse("Start offset should not be the last index of 1st snapshot", start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be 1st snapshot",
        snapshotIds.get(0).longValue(), end.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end.index());
    Assert.assertTrue("End offset's snapshot id should be a starting snapshot id", end.isStartingSnapshotId());
    Assert.assertTrue("End offset should be the last index of 1st snapshot", end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be 2nd snapshot",
        snapshotIds.get(1).longValue(), end1.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end1.index());
    Assert.assertFalse("End offset's snapshot id should not be a starting snapshot id", end1.isStartingSnapshotId());
    Assert.assertTrue("End offset should be the last index of 2nd snapshot", end1.isLastIndexOfSnapshot());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpecifyInvalidSnapshotId() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();
    IcebergSource source = new IcebergSource();

    // test invalid snapshot id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", "-1"));
    AssertHelpers.assertThrows("Test invalid snapshot id",
        IllegalStateException.class, "The option starting-snapshot-id -1 is not an ancestor",
        () -> source.createMicroBatchReader(Optional.empty(), checkpoint.toString(), options));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSpecifySnapshotId() throws IOException {
    File checkpoint = Files.createTempDirectory(parent, "checkpoint").toFile();

    IcebergSource source = new IcebergSource();
    List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
    Collections.reverse(snapshotIds);

    // test specify snapshot-id
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", tableLocation.toString(),
        "checkpointLocation", checkpoint.toString(),
        "starting-snapshot-id", snapshotIds.get(1).toString(),
        "max-size-per-batch", "1000"));
    StreamingReader streamingReader = (StreamingReader) source.createMicroBatchReader(
        Optional.empty(), checkpoint.toString(), options);

    streamingReader.setOffsetRange(Optional.empty(), Optional.empty());
    StreamingOffset start = (StreamingOffset) streamingReader.getStartOffset();
    Assert.assertEquals("Start offset's snapshot id should be 2nd snapshot id",
        snapshotIds.get(1).longValue(), start.snapshotId());
    Assert.assertEquals("Start offset's index should be the start index of 2nd snapshot", 0, start.index());
    Assert.assertTrue("Start offset's snapshot id should be a starting snapshot id", start.isStartingSnapshotId());
    Assert.assertFalse("Start offset should not be the last index of 2nd snapshot", start.isLastIndexOfSnapshot());

    StreamingOffset end = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be 2nd snapshot",
        snapshotIds.get(1).longValue(), end.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end.index());
    Assert.assertTrue("End offset's snapshot id should be a starting snapshot id", end.isStartingSnapshotId());
    Assert.assertFalse("End offset should not be the last index of 2nd snapshot", end.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end), Optional.empty());
    StreamingOffset end1 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be 3rd snapshot",
        snapshotIds.get(2).longValue(), end1.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end1.index());
    Assert.assertFalse("End offset's snapshot id should not be a starting snapshot id", end1.isStartingSnapshotId());
    Assert.assertTrue("End offset should not be the last index of 3rd snapshot", end1.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end1), Optional.empty());
    StreamingOffset end2 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset's snapshot id should be 4th snapshot",
        snapshotIds.get(3).longValue(), end2.snapshotId());
    Assert.assertEquals("End offset's index should be the last index", 1, end2.index());
    Assert.assertFalse("End offset's snapshot id should not be a starting snapshot id", end2.isStartingSnapshotId());
    Assert.assertTrue("End offset should not be the last index of 4th snapshot", end2.isLastIndexOfSnapshot());

    streamingReader.setOffsetRange(Optional.of(end2), Optional.empty());
    StreamingOffset end3 = (StreamingOffset) streamingReader.getEndOffset();
    Assert.assertEquals("End offset should be same to start offset since there's no more batches to consume",
        end2, end3);
  }

  public DataFile writeFile(StructLike partition, List<Record> records) {
    Preconditions.checkNotNull(table, "table not set");
    try {
      return writeFile(table, partition, records, FileFormat.PARQUET, parent.toFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static DataFile writeFile(Table table, StructLike partition, List<Record> records, FileFormat fileFormat,
                                   File file) throws IOException {
    Assert.assertTrue(file.delete());

    FileAppender<Record> appender;

    switch (fileFormat) {
      case AVRO:
        appender = Avro.write(org.apache.iceberg.Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(DataWriter::create)
                .named(fileFormat.name())
                .build();
        break;

      case PARQUET:
        appender = Parquet.write(org.apache.iceberg.Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .named(fileFormat.name())
                .build();
        break;

      case ORC:
        appender = ORC.write(org.apache.iceberg.Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .build();
        break;

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }

    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFiles.Builder builder = DataFiles.builder(table.spec())
            .withPath(file.toString())
            .withFormat(fileFormat)
            .withFileSizeInBytes(file.length())
            .withMetrics(appender.metrics());

    if (partition != null) {
      builder.withPartition(partition);
    }

    return builder.build();
  }
}
