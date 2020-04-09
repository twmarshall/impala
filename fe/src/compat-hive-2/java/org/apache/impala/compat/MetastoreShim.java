// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.compat;

import static org.apache.impala.service.MetadataOp.TABLE_TYPE_TABLE;
import static org.apache.impala.service.MetadataOp.TABLE_TYPE_VIEW;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventRequestData;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.json.ExtendedJSONMessageFactory;
import org.apache.hive.service.rpc.thrift.TGetColumnsReq;
import org.apache.hive.service.rpc.thrift.TGetFunctionsReq;
import org.apache.hive.service.rpc.thrift.TGetSchemasReq;
import org.apache.hive.service.rpc.thrift.TGetTablesReq;
import org.apache.impala.authorization.User;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.TransactionException;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.MetadataOp;
import org.apache.impala.thrift.TMetadataOpRequest;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.util.AcidUtils.TblTransaction;
import org.apache.impala.util.MetaStoreUtil.InsertEventInfo;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

/**
 * A wrapper around some of Hive's Metastore API's to abstract away differences
 * between major versions of Hive. This implements the shimmed methods for Hive 2.
 */
public class MetastoreShim {
  private static final Logger LOG = Logger.getLogger(MetastoreShim.class);

  public static TblTransaction createTblTransaction(
     IMetaStoreClient client, Table tbl, long txnId) {
    throw new UnsupportedOperationException("createTblTransaction");
  }

  public static void commitTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) throws TransactionException {
    throw new UnsupportedOperationException("commitTblTransactionIfNeeded");
  }

  public static void abortTblTransactionIfNeeded(IMetaStoreClient client,
      TblTransaction tblTxn) {
    throw new UnsupportedOperationException("abortTblTransactionIfNeeded");
  }

  /**
   * Wrapper around MetaStoreUtils.validateName() to deal with added arguments.
   */
  public static boolean validateName(String name) {
    return MetaStoreUtils.validateName(name, null);
  }

  /**
   * Hive-3 only function
   */
  public static void alterTableWithTransaction(IMetaStoreClient client,
      Table tbl, TblTransaction tblTxn) {
    throw new UnsupportedOperationException("alterTableWithTransaction");
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partition() to deal with added
   * arguments.
   */
  public static void alterPartition(IMetaStoreClient client, Partition partition)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partition(
        partition.getDbName(), partition.getTableName(), partition, null);
  }

  /**
   * Wrapper around IMetaStoreClient.alter_partitions() to deal with added
   * arguments.
   */
  public static void alterPartitions(IMetaStoreClient client, String dbName,
      String tableName, List<Partition> partitions)
      throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(dbName, tableName, partitions, null);
  }

  /**
   * Wrapper around IMetaStoreClient.createTableWithConstraints() to deal with added
   * arguments.
   */
  public static void createTableWithConstraints(IMetaStoreClient client,
      Table newTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
      throws InvalidOperationException, MetaException, TException {
    client.createTableWithConstraints(newTbl, primaryKeys, foreignKeys);
  }

 /**
  * Hive-3 only function
  */
  public static void alterPartitionsWithTransaction(IMetaStoreClient client,
      String dbName, String tblName, List<Partition> partitions, TblTransaction tblTxn) {
    throw new UnsupportedOperationException("alterTableWithTransaction");
  }

  /**
   * Wrapper around IMetaStoreClient.getTableColumnStatistics() to deal with added
   * arguments.
   */
  public static List<ColumnStatisticsObj> getTableColumnStatistics(
      IMetaStoreClient client, String dbName, String tableName, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException {
    return client.getTableColumnStatistics(dbName, tableName, colNames);
  }

  /**
   * Wrapper around IMetaStoreClient.deleteTableColumnStatistics() to deal with added
   * arguments.
   */
  public static boolean deleteTableColumnStatistics(IMetaStoreClient client,
      String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException,
             InvalidInputException {
    return client.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  /**
   * Wrapper around ColumnStatistics c'tor to deal with the added engine property.
   */
  public static ColumnStatistics createNewHiveColStats() {
    return new ColumnStatistics();
  }

  /**
   * Wrapper around MetaStoreUtils.updatePartitionStatsFast() to deal with added
   * arguments.
   */
  public static void updatePartitionStatsFast(Partition partition, Table tbl,
      Warehouse warehouse) throws MetaException {
    MetaStoreUtils.updatePartitionStatsFast(partition, warehouse, null);
  }

  /**
   * Return the maximum number of Metastore objects that should be retrieved in
   * a batch.
   */
  public static String metastoreBatchRetrieveObjectsMaxConfigKey() {
    return HiveConf.ConfVars.METASTORE_BATCH_RETRIEVE_OBJECTS_MAX.toString();
  }

  /**
   * Return the key and value that should be set in the partition parameters to
   * mark that the stats were generated automatically by a stats task.
   */
  public static Pair<String, String> statsGeneratedViaStatsTaskParam() {
    return Pair.create(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
  }

  public static TResultSet execGetFunctions(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetFunctionsReq req = request.getGet_functions_req();
    return MetadataOp.getFunctions(
        frontend, req.getCatalogName(), req.getSchemaName(), req.getFunctionName(), user);
  }

  public static TResultSet execGetColumns(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetColumnsReq req = request.getGet_columns_req();
    return MetadataOp.getColumns(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getColumnName(), user);
  }

  public static TResultSet execGetTables(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetTablesReq req = request.getGet_tables_req();
    return MetadataOp.getTables(frontend, req.getCatalogName(), req.getSchemaName(),
        req.getTableName(), req.getTableTypes(), user);
  }

  public static TResultSet execGetSchemas(
      Frontend frontend, TMetadataOpRequest request, User user) throws ImpalaException {
    TGetSchemasReq req = request.getGet_schemas_req();
    return MetadataOp.getSchemas(
        frontend, req.getCatalogName(), req.getSchemaName(), user);
  }

  /**
   * Supported HMS-2 types
   */
  public static final EnumSet<TableType> IMPALA_SUPPORTED_TABLE_TYPES = EnumSet
      .of(TableType.EXTERNAL_TABLE, TableType.MANAGED_TABLE, TableType.VIRTUAL_VIEW);

  /**
   * mapping between the HMS-2 type the Impala types
   */
  public static final ImmutableMap<String, String> HMS_TO_IMPALA_TYPE =
      new ImmutableMap.Builder<String, String>()
          .put("EXTERNAL_TABLE", TABLE_TYPE_TABLE)
          .put("MANAGED_TABLE", TABLE_TYPE_TABLE)
          .put("INDEX_TABLE", TABLE_TYPE_TABLE)
          .put("VIRTUAL_VIEW", TABLE_TYPE_VIEW).build();

  public static String mapToInternalTableType(String typeStr) {
    String defaultTableType = TABLE_TYPE_TABLE;

    TableType tType;

    if (typeStr == null) return defaultTableType;
    try {
      tType = TableType.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      return defaultTableType;
    }
    switch (tType) {
      case EXTERNAL_TABLE:
      case MANAGED_TABLE:
      case INDEX_TABLE:
        return TABLE_TYPE_TABLE;
      case VIRTUAL_VIEW:
        return TABLE_TYPE_VIEW;
      default:
        return defaultTableType;
    }

  }

  /**
   * Wrapper method which returns ExtendedJSONMessageFactory in case Impala is
   * building against Hive-2 to keep compatibility with Sentry
   */
  public static MessageDeserializer getMessageDeserializer() {
    return ExtendedJSONMessageFactory.getInstance().getDeserializer();
  }

  /**
   * Wrapper around FileUtils.makePartName to deal with package relocation in Hive 3
   * @param partitionColNames
   * @param values
   * @return
   */
  public static String makePartName(List<String> partitionColNames, List<String> values) {
    return FileUtils.makePartName(partitionColNames, values);
  }

  /**
   * Wrapper method around message factory's build alter table message due to added
   * arguments in hive 3.
   */
  @VisibleForTesting
  public static AlterTableMessage buildAlterTableMessage(Table before, Table after,
      boolean isTruncateOp, long writeId) {
    Preconditions.checkArgument(writeId < 0, "Write ids are not supported in Hive-2 "
        + "compatible build");
    Preconditions.checkArgument(!isTruncateOp, "Truncate operation is not supported in "
        + "alter table messages in Hive-2 compatible build");
    return ExtendedJSONMessageFactory.getInstance().buildAlterTableMessage(before, after);
  }

  /**
   * Wrapper around HMS-2 message serializer
   * @param message
   * @return serialized string to use used in the NotificationEvent's message field
   */
  @VisibleForTesting
  public static String serializeEventMessage(EventMessage message) {
    return message.toString();
  }

  public static String getAllColumnsInformation(List<FieldSchema> tabCols,
      List<FieldSchema> partitionCols, boolean printHeader, boolean isOutputPadded,
      boolean showPartColsSeparately) {
    return MetaDataFormatUtils
        .getAllColumnsInformation(tabCols, partitionCols, printHeader, isOutputPadded,
            showPartColsSeparately);
  }

  /**
   * Wrapper method around Hive's MetadataFormatUtils.getTableInformation which has
   * changed significantly in Hive-3
   * @return
   */
  public static String getTableInformation(
      org.apache.hadoop.hive.ql.metadata.Table table) {
    return MetaDataFormatUtils.getTableInformation(table);
  }

  /**
   * Wrapper method around BaseSemanticAnalyzer's unespaceSQLString to be compatibility
   * with Hive. Takes in a normalized value of the string surrounded by single quotes
   */
  public static String unescapeSQLString(String normalizedStringLiteral) {
    return BaseSemanticAnalyzer.unescapeSQLString(normalizedStringLiteral);
  }

  /**
   * This is Hive-3 only function
   */
  public static ValidWriteIdList fetchValidWriteIds(IMetaStoreClient client,
      String tableFullName) {
    throw new UnsupportedOperationException("fetchValidWriteIds not supported");
  }

  /**
   * Hive-3 only function
   */
  public static ValidWriteIdList getValidWriteIdListFromString(String validWriteIds) {
    throw new UnsupportedOperationException(
        "getValidWriteIdListFromString not supported");
  }

  /**
   * Hive-3 only function
   */
  public static ValidTxnList getValidTxns(IMetaStoreClient client) throws TException {
    throw new UnsupportedOperationException("getValidTxns not supported");
  }

  /**
   * Hive-3 only function
   * -1 means undefined
   */
  public static long getWriteIdFromMSPartition(Partition partition) {
    return -1L;
  }

  /**
   *  Hive-3 only function
   */
  public static void setWriteIdForMSPartition(Partition partition, long writeId) {
  }

  /**
   *  Hive-3 only function
   *  -1 means undefined
   */
  public static long getWriteIdFromMSTable(Table msTbl) {
    return -1L;
  }

  public static boolean hasTableCapability(Table msTbl, byte requiredCapability) {
    throw new UnsupportedOperationException("hasTableCapability not supported");
  }

  public static String getTableAccessType(Table msTbl) {
    throw new UnsupportedOperationException("getTableAccessType not supported");
  }

  public static void setTableAccessType(Table msTbl, byte accessType) {
    throw new UnsupportedOperationException("setTableAccessType not supported");
  }

  public static void setHiveClientCapabilities() {
    throw new UnsupportedOperationException("setHiveClientCapabilities not supported");
  }

  /**
   * Hive-3 only function
   */
  public static long openTransaction(IMetaStoreClient client)
      throws TransactionException {
    throw new UnsupportedOperationException("openTransaction is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static void commitTransaction(IMetaStoreClient client, long txnId)
      throws TransactionException {
    throw new UnsupportedOperationException("commitTransaction is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static void abortTransaction(IMetaStoreClient client, long txnId)
      throws TransactionException {
    throw new UnsupportedOperationException("abortTransaction is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static void releaseLock(IMetaStoreClient client, long lockId)
      throws TransactionException {
    throw new UnsupportedOperationException("releaseLock is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static boolean heartbeat(IMetaStoreClient client,
      long txnId, long lockId) throws TransactionException {
    throw new UnsupportedOperationException("heartbeat is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static long acquireLock(IMetaStoreClient client, long txnId,
      List<LockComponent> lockComponents)
      throws TransactionException {
    throw new UnsupportedOperationException("acquireLock is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static long allocateTableWriteId(IMetaStoreClient client, long txnId,
      String dbName, String tableName) throws TransactionException {
    throw new UnsupportedOperationException("allocateTableWriteId is not supported.");
  }

  /**
   * Hive-3 only function
   */
  public static void setTableColumnStatsTransactional(IMetaStoreClient client,
      Table msTbl, ColumnStatistics colStats, TblTransaction tblTxn)
      throws ImpalaRuntimeException {
    throw new UnsupportedOperationException(
        "setTableColumnStatsTransactional is not supported.");
  }

  /**
   * @return the shim version.
   */
  public static long getMajorVersion() {
    return 2;
  }

  /**
   * Return the default table path for a new table.
   *
   * Hive-3 doesn't allow managed table to be non transactional after HIVE-22158.
   * Creating a non transactional managed table will finally result in an external table
   * with table property "external.table.purge" set to true. As the table type become
   * EXTERNAL, the location will be under "metastore.warehouse.external.dir" (HIVE-19837,
   * introduces in hive-2.7, not in hive-2.1.x-cdh6.x yet).
   */
  public static String getPathForNewTable(Database db, Table tbl)
      throws MetaException {
    return new Path(db.getLocationUri(), tbl.getTableName().toLowerCase()).toString();
  }

  /**
   * Fire insert events asynchronously. This creates a single thread to execute the
   * fireInsertEvent method and shuts down the thread after it has finished.
   * In case of any exception, we just log the failure of firing insert events.
   */
  public static List<Long> fireInsertEvents(MetaStoreClient msClient,
      List<InsertEventInfo> insertEventInfos, String dbName, String tableName) {
    ExecutorService fireInsertEventThread = Executors.newSingleThreadExecutor();
    CompletableFuture.runAsync(() -> {
      try {
        fireInsertEventHelper(msClient.getHiveClient(), insertEventInfos, dbName, tableName);
      } catch (Exception e) {
        LOG.error("Failed to fire insert event. Some tables might not be"
                + " refreshed on other impala clusters.", e);
      } finally {
        msClient.close();
      }
    }, Executors.newSingleThreadExecutor()).thenRun(() ->
        fireInsertEventThread.shutdown());
    return Collections.emptyList();
  }

  /**
   *  Fires an insert event to HMS notification log. In Hive-2 for partitioned table,
   *  each existing partition touched by the insert will fire a separate insert event.
   * @param msClient Metastore client,
   * @param insertEventInfos A list of insert event encapsulating the information needed
   * to fire insert
   * @param dbName
   * @param tableName
   */
  @VisibleForTesting
  public static void fireInsertEventHelper(IMetaStoreClient msClient,
      List<InsertEventInfo> insertEventInfos, String dbName, String tableName)
      throws TException {
    Preconditions.checkNotNull(msClient);
    Preconditions.checkNotNull(dbName);
    Preconditions.checkNotNull(tableName);
    for (InsertEventInfo info : insertEventInfos) {
      Preconditions.checkNotNull(info.getNewFiles());
      LOG.debug("Firing an insert event for " + tableName);
      FireEventRequestData data = new FireEventRequestData();
      InsertEventRequestData insertData = new InsertEventRequestData();
      data.setInsertData(insertData);
      FireEventRequest rqst = new FireEventRequest(true, data);
      rqst.setDbName(dbName);
      rqst.setTableName(tableName);
      insertData.setFilesAdded(new ArrayList<>(info.getNewFiles()));
      insertData.setReplace(info.isOverwrite());
      if (info.getPartVals() != null) rqst.setPartitionVals(info.getPartVals());
      msClient.fireListenerEvent(rqst);
    }
  }
}
