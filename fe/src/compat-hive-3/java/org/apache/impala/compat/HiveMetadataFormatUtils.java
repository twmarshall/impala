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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.EntityArrays;
import org.apache.commons.lang3.text.translate.LookupTranslator;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.serde2.io.DateWritable;

/**
 * Most of the code in this class is copied from Hive 2.1.1. This is used so that
 * the describe table output from Impala matches to Hive as much as possible. Initially,
 * Impala had a dependency with hive-exec which pulled in this class directly from
 * hive-exec jar. But in Hive 3 this code has diverged a lot and getting it from hive-exec
 * pulls in a lot of unnecessary dependencies. It could be argued that
 * supporting describe table to be similar to Hive's describe table does not make much
 * sense. Since the code has diverged anyways when compared to Hive 3, we should maintain
 * our own code from now on and make changes as required.
 */
public class HiveMetadataFormatUtils {

  public static final String FIELD_DELIM = "\t";
  public static final String LINE_DELIM = "\n";

  static final int DEFAULT_STRINGBUILDER_SIZE = 2048;
  private static final int ALIGNMENT = 20;

  /**
   * Write formatted information about the given columns, including partition columns to a
   * string
   *
   * @param cols - list of columns
   * @param partCols - list of partition columns
   * @param printHeader - if header should be included
   * @param isOutputPadded - make it more human readable by setting indentation with
   *     spaces. Turned off for use by HiveServer2
   * @return string with formatted column information
   */
  public static String getAllColumnsInformation(List<FieldSchema> cols,
      List<FieldSchema> partCols, boolean printHeader, boolean isOutputPadded,
      boolean showPartColsSep) {
    StringBuilder columnInformation = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    if (printHeader) {
      formatColumnsHeader(columnInformation, null);
    }
    formatAllFields(columnInformation, cols, isOutputPadded, null);

    if ((partCols != null) && !partCols.isEmpty() && showPartColsSep) {
      columnInformation.append(LINE_DELIM).append("# Partition Information")
          .append(LINE_DELIM);
      formatColumnsHeader(columnInformation, null);
      formatAllFields(columnInformation, partCols, isOutputPadded, null);
    }

    return columnInformation.toString();
  }

  private static void formatColumnsHeader(StringBuilder columnInformation,
      List<ColumnStatisticsObj> colStats) {
    columnInformation.append("# "); // Easy for shell scripts to ignore
    formatOutput(getColumnsHeader(colStats), columnInformation, false, true);
    columnInformation.append(LINE_DELIM);
  }

  /**
   * Prints a row with the given fields into the builder The last field could be a
   * multiline field, and the extra lines should be padded
   *
   * @param fields The fields to print
   * @param tableInfo The target builder
   * @param isLastLinePadded Is the last field could be printed in multiple lines, if
   *     contains newlines?
   */
  private static void formatOutput(String[] fields, StringBuilder tableInfo,
      boolean isLastLinePadded, boolean isFormatted) {
    if (!isFormatted) {
      for (int i = 0; i < fields.length; i++) {
        Object value = StringEscapeUtils.escapeJava(fields[i]);
        if (value != null) {
          tableInfo.append(value);
        }
        tableInfo.append((i == fields.length - 1) ? LINE_DELIM : FIELD_DELIM);
      }
    } else {
      int[] paddings = new int[fields.length - 1];
      if (fields.length > 1) {
        for (int i = 0; i < fields.length - 1; i++) {
          if (fields[i] == null) {
            tableInfo.append(FIELD_DELIM);
            continue;
          }
          tableInfo.append(String.format("%-" + ALIGNMENT + "s", fields[i]))
              .append(FIELD_DELIM);
          paddings[i] = ALIGNMENT > fields[i].length() ? ALIGNMENT : fields[i].length();
        }
      }
      if (fields.length > 0) {
        String value = fields[fields.length - 1];
        String unescapedValue = (isLastLinePadded && value != null) ? value
            .replaceAll("\\\\n|\\\\r|\\\\r\\\\n", "\n") : value;
        indentMultilineValue(unescapedValue, tableInfo, paddings, false);
      } else {
        tableInfo.append(LINE_DELIM);
      }
    }
  }

  private static final String schema = "col_name,data_type,comment#string:string:string";
  private static final String colStatsSchema = "col_name,data_type,min,max,num_nulls,"
      + "distinct_count,avg_col_len,max_col_len,num_trues,num_falses,comment"
      + "#string:string:string:string:string:string:string:string:string:string:string";

  public static String[] getColumnsHeader(List<ColumnStatisticsObj> colStats) {
    String colSchema = schema;
    if (colStats != null) {
      colSchema = colStatsSchema;
    }
    return colSchema.split("#")[0].split(",");
  }

  /**
   * Write formatted column information into given StringBuilder
   *
   * @param tableInfo - StringBuilder to append column information into
   * @param cols - list of columns
   * @param isOutputPadded - make it more human readable by setting indentation with
   *     spaces. Turned off for use by HiveServer2
   */
  private static void formatAllFields(StringBuilder tableInfo, List<FieldSchema> cols,
      boolean isOutputPadded, List<ColumnStatisticsObj> colStats) {
    for (FieldSchema col : cols) {
      if (isOutputPadded) {
        formatWithIndentation(col.getName(), col.getType(), getComment(col), tableInfo,
            colStats);
      } else {
        formatWithoutIndentation(col.getName(), col.getType(), col.getComment(),
            tableInfo, colStats);
      }
    }
  }

  private static void formatWithoutIndentation(String name, String type, String comment,
      StringBuilder colBuffer, List<ColumnStatisticsObj> colStats) {
    colBuffer.append(name);
    colBuffer.append(FIELD_DELIM);
    colBuffer.append(type);
    colBuffer.append(FIELD_DELIM);
    if (colStats != null) {
      ColumnStatisticsObj cso = getColumnStatisticsObject(name, type, colStats);
      if (cso != null) {
        ColumnStatisticsData csd = cso.getStatsData();
        if (csd.isSetBinaryStats()) {
          BinaryColumnStatsData bcsd = csd.getBinaryStats();
          appendColumnStatsNoFormatting(colBuffer, "", "", bcsd.getNumNulls(), "",
              bcsd.getAvgColLen(), bcsd.getMaxColLen(), "", "");
        } else if (csd.isSetStringStats()) {
          StringColumnStatsData scsd = csd.getStringStats();
          appendColumnStatsNoFormatting(colBuffer, "", "", scsd.getNumNulls(),
              scsd.getNumDVs(), scsd.getAvgColLen(), scsd.getMaxColLen(), "", "");
        } else if (csd.isSetBooleanStats()) {
          BooleanColumnStatsData bcsd = csd.getBooleanStats();
          appendColumnStatsNoFormatting(colBuffer, "", "", bcsd.getNumNulls(), "", "", "",
              bcsd.getNumTrues(), bcsd.getNumFalses());
        } else if (csd.isSetDecimalStats()) {
          DecimalColumnStatsData dcsd = csd.getDecimalStats();
          appendColumnStatsNoFormatting(colBuffer, convertToString(dcsd.getLowValue()),
              convertToString(dcsd.getHighValue()), dcsd.getNumNulls(), dcsd.getNumDVs(),
              "", "", "", "");
        } else if (csd.isSetDoubleStats()) {
          DoubleColumnStatsData dcsd = csd.getDoubleStats();
          appendColumnStatsNoFormatting(colBuffer, dcsd.getLowValue(),
              dcsd.getHighValue(), dcsd.getNumNulls(), dcsd.getNumDVs(), "", "", "", "");
        } else if (csd.isSetLongStats()) {
          LongColumnStatsData lcsd = csd.getLongStats();
          appendColumnStatsNoFormatting(colBuffer, lcsd.getLowValue(),
              lcsd.getHighValue(), lcsd.getNumNulls(), lcsd.getNumDVs(), "", "", "", "");
        } else if (csd.isSetDateStats()) {
          DateColumnStatsData dcsd = csd.getDateStats();
          appendColumnStatsNoFormatting(colBuffer, convertToString(dcsd.getLowValue()),
              convertToString(dcsd.getHighValue()), dcsd.getNumNulls(), dcsd.getNumDVs(),
              "", "", "", "");
        }
      } else {
        appendColumnStatsNoFormatting(colBuffer, "", "", "", "", "", "", "", "");
      }
    }
    colBuffer.append(comment == null ? "" : ESCAPE_JAVA.translate(comment));
    colBuffer.append(LINE_DELIM);
  }

  private static final CharSequenceTranslator ESCAPE_JAVA =
      new LookupTranslator(new String[][]{{"\"", "\\\""}, {"\\", "\\\\"},})
          .with(new LookupTranslator(EntityArrays.JAVA_CTRL_CHARS_ESCAPE()));

  private static void appendColumnStatsNoFormatting(StringBuilder sb, Object min,
      Object max, Object numNulls, Object ndv, Object avgColLen, Object maxColLen,
      Object numTrues, Object numFalses) {
    sb.append(min).append(FIELD_DELIM);
    sb.append(max).append(FIELD_DELIM);
    sb.append(numNulls).append(FIELD_DELIM);
    sb.append(ndv).append(FIELD_DELIM);
    sb.append(avgColLen).append(FIELD_DELIM);
    sb.append(maxColLen).append(FIELD_DELIM);
    sb.append(numTrues).append(FIELD_DELIM);
    sb.append(numFalses).append(FIELD_DELIM);
  }

  static String getComment(FieldSchema col) {
    return col.getComment() != null ? col.getComment() : "";
  }

  private static void formatWithIndentation(String colName, String colType,
      String colComment, StringBuilder tableInfo, List<ColumnStatisticsObj> colStats) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", colName)).append(FIELD_DELIM);
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", colType)).append(FIELD_DELIM);

    if (colStats != null) {
      ColumnStatisticsObj cso = getColumnStatisticsObject(colName, colType, colStats);
      if (cso != null) {
        ColumnStatisticsData csd = cso.getStatsData();
        if (csd.isSetBinaryStats()) {
          BinaryColumnStatsData bcsd = csd.getBinaryStats();
          appendColumnStats(tableInfo, "", "", bcsd.getNumNulls(), "",
              bcsd.getAvgColLen(), bcsd.getMaxColLen(), "", "");
        } else if (csd.isSetStringStats()) {
          StringColumnStatsData scsd = csd.getStringStats();
          appendColumnStats(tableInfo, "", "", scsd.getNumNulls(), scsd.getNumDVs(),
              scsd.getAvgColLen(), scsd.getMaxColLen(), "", "");
        } else if (csd.isSetBooleanStats()) {
          BooleanColumnStatsData bcsd = csd.getBooleanStats();
          appendColumnStats(tableInfo, "", "", bcsd.getNumNulls(), "", "", "",
              bcsd.getNumTrues(), bcsd.getNumFalses());
        } else if (csd.isSetDecimalStats()) {
          DecimalColumnStatsData dcsd = csd.getDecimalStats();
          appendColumnStats(tableInfo, convertToString(dcsd.getLowValue()),
              convertToString(dcsd.getHighValue()), dcsd.getNumNulls(), dcsd.getNumDVs(),
              "", "", "", "");
        } else if (csd.isSetDoubleStats()) {
          DoubleColumnStatsData dcsd = csd.getDoubleStats();
          appendColumnStats(tableInfo, dcsd.getLowValue(), dcsd.getHighValue(),
              dcsd.getNumNulls(), dcsd.getNumDVs(), "", "", "", "");
        } else if (csd.isSetLongStats()) {
          LongColumnStatsData lcsd = csd.getLongStats();
          appendColumnStats(tableInfo, lcsd.getLowValue(), lcsd.getHighValue(),
              lcsd.getNumNulls(), lcsd.getNumDVs(), "", "", "", "");
        } else if (csd.isSetDateStats()) {
          DateColumnStatsData dcsd = csd.getDateStats();
          appendColumnStats(tableInfo, convertToString(dcsd.getLowValue()),
              convertToString(dcsd.getHighValue()), dcsd.getNumNulls(), dcsd.getNumDVs(),
              "", "", "", "");
        }
      } else {
        appendColumnStats(tableInfo, "", "", "", "", "", "", "", "");
      }
    }

    int colNameLength = ALIGNMENT > colName.length() ? ALIGNMENT : colName.length();
    int colTypeLength = ALIGNMENT > colType.length() ? ALIGNMENT : colType.length();
    indentMultilineValue(colComment, tableInfo, new int[]{colNameLength, colTypeLength},
        false);
  }

  /**
   * comment indent processing for multi-line values values should be indented the same
   * amount on each line if the first line comment starts indented by k, the following
   * line comments should also be indented by k
   *
   * @param value the value to write
   * @param tableInfo the buffer to write to
   * @param columnWidths the widths of the previous columns
   * @param printNull print null as a string, or do not print anything
   */
  private static void indentMultilineValue(String value, StringBuilder tableInfo,
      int[] columnWidths, boolean printNull) {
    if (value == null) {
      if (printNull) {
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", value));
      }
      tableInfo.append(LINE_DELIM);
    } else {
      String[] valueSegments = value.split("\n|\r|\r\n");
      tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[0]))
          .append(LINE_DELIM);
      for (int i = 1; i < valueSegments.length; i++) {
        printPadding(tableInfo, columnWidths);
        tableInfo.append(String.format("%-" + ALIGNMENT + "s", valueSegments[i]))
            .append(LINE_DELIM);
      }
    }
  }

  /**
   * Print the rigth padding, with the given column widths
   *
   * @param tableInfo The buffer to write to
   * @param columnWidths The column widths
   */
  private static void printPadding(StringBuilder tableInfo, int[] columnWidths) {
    for (int columnWidth : columnWidths) {
      if (columnWidth == 0) {
        tableInfo.append(FIELD_DELIM);
      } else {
        tableInfo.append(String.format("%" + columnWidth + "s" + FIELD_DELIM, ""));
      }
    }
  }

  private static String convertToString(Decimal val) {
    if (val == null) {
      return "";
    }

    HiveDecimal result =
        HiveDecimal.create(new BigInteger(val.getUnscaled()), val.getScale());
    if (result != null) {
      return result.toString();
    } else {
      return "";
    }
  }

  private static String convertToString(org.apache.hadoop.hive.metastore.api.Date val) {
    if (val == null) {
      return "";
    }

    DateWritable writableValue = new DateWritable((int) val.getDaysSinceEpoch());
    return writableValue.toString();
  }

  private static void appendColumnStats(StringBuilder sb, Object min, Object max,
      Object numNulls, Object ndv, Object avgColLen, Object maxColLen, Object numTrues,
      Object numFalses) {
    sb.append(String.format("%-" + ALIGNMENT + "s", min)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", max)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", numNulls)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", ndv)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", avgColLen)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", maxColLen)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", numTrues)).append(FIELD_DELIM);
    sb.append(String.format("%-" + ALIGNMENT + "s", numFalses)).append(FIELD_DELIM);
  }

  private static ColumnStatisticsObj getColumnStatisticsObject(String colName,
      String colType, List<ColumnStatisticsObj> colStats) {
    if (colStats != null && !colStats.isEmpty()) {
      for (ColumnStatisticsObj cso : colStats) {
        if (cso.getColName().equalsIgnoreCase(colName) && cso.getColType()
            .equalsIgnoreCase(colType)) {
          return cso;
        }
      }
    }
    return null;
  }

  public static String getConstraintsInformation(
      org.apache.hadoop.hive.ql.metadata.Table table) {
    StringBuilder constraintsInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    constraintsInfo.append(LINE_DELIM).append("# Constraints").append(LINE_DELIM);

    if (PrimaryKeyInfo.isPrimaryKeyInfoNotEmpty(table.getPrimaryKeyInfo())) {
      constraintsInfo.append(LINE_DELIM).append("# Primary Key").append(LINE_DELIM);
      getPrimaryKeyInformation(constraintsInfo, table.getPrimaryKeyInfo());
    }
    if (ForeignKeyInfo.isForeignKeyInfoNotEmpty(table.getForeignKeyInfo())) {
      constraintsInfo.append(LINE_DELIM).append("# Foreign Keys").append(LINE_DELIM);
      getForeignKeysInformation(constraintsInfo, table.getForeignKeyInfo());
    }

    return constraintsInfo.toString();
  }

  private static void getPrimaryKeyInformation(StringBuilder constraintsInfo,
      PrimaryKeyInfo pkInfo) {
    formatOutput("Table:", pkInfo.getDatabaseName() + "." + pkInfo.getTableName(),
        constraintsInfo);
    formatOutput("Constraint Name:", pkInfo.getConstraintName(), constraintsInfo);
    Map<Integer, String> colNames = pkInfo.getColNames();
    final String title = "Column Name:".intern();
    for (String colName : colNames.values()) {
      constraintsInfo.append(String.format("%-" + ALIGNMENT + "s", title))
          .append(FIELD_DELIM);
      formatOutput(new String[] {colName}, constraintsInfo);
    }
  }

  private static void getForeignKeyColInformation(StringBuilder constraintsInfo,
    ForeignKeyInfo.ForeignKeyCol fkCol) {
      String[] fkcFields = new String[3];
      fkcFields[0] = "Parent Column Name:" + fkCol.parentDatabaseName +
          "."+ fkCol.parentTableName + "." + fkCol.parentColName;
      fkcFields[1] = "Column Name:" + fkCol.childColName;
      fkcFields[2] = "Key Sequence:" + fkCol.position;
      formatOutput(fkcFields, constraintsInfo);
  }

  private static void getForeignKeyRelInformation(
    StringBuilder constraintsInfo,
    String constraintName,
    List<ForeignKeyInfo.ForeignKeyCol> fkRel) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (fkRel != null && fkRel.size() > 0) {
      for (ForeignKeyInfo.ForeignKeyCol fkc : fkRel) {
        getForeignKeyColInformation(constraintsInfo, fkc);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private static void getForeignKeysInformation(StringBuilder constraintsInfo,
      ForeignKeyInfo fkInfo) {
    formatOutput("Table:",
        fkInfo.getChildDatabaseName() + "." + fkInfo.getChildTableName(),
        constraintsInfo);
    Map<String, List<ForeignKeyInfo.ForeignKeyCol>> foreignKeys = fkInfo.getForeignKeys();
    if (foreignKeys != null && foreignKeys.size() > 0) {
      for (Map.Entry<String, List<ForeignKeyInfo.ForeignKeyCol>> me : foreignKeys
          .entrySet()) {
        getForeignKeyRelInformation(constraintsInfo, me.getKey(), me.getValue());
      }
    }
  }

  public static String getTableInformation(Table table, boolean isOutputPadded) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append(LINE_DELIM).append("# Detailed Table Information")
        .append(LINE_DELIM);
    getTableMetaDataInformation(tableInfo, table, isOutputPadded);


    // Storage information.
    tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
    getStorageDescriptorInfo(tableInfo, table.getSd());

    if (TableType.VIRTUAL_VIEW.equals(TableType.valueOf(table.getTableType()))) {
      tableInfo.append(LINE_DELIM).append("# View Information").append(LINE_DELIM);
      getViewInfo(tableInfo, table);
    }

    return tableInfo.toString();
  }

  private static void getViewInfo(StringBuilder tableInfo, Table tbl) {
    formatOutput("View Original Text:", tbl.getViewOriginalText(), tableInfo);
    formatOutput("View Expanded Text:", tbl.getViewExpandedText(), tableInfo);
  }

  private static void getTableMetaDataInformation(StringBuilder tableInfo, Table tbl,
      boolean isOutputPadded) {
    formatOutput("Database:", tbl.getDbName(), tableInfo);
    formatOutput("OwnerType:",
        (tbl.getOwnerType() != null) ? tbl.getOwnerType().name() : "null", tableInfo);
    formatOutput("Owner:", tbl.getOwner(), tableInfo);
    formatOutput("CreateTime:", formatDate(tbl.getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(tbl.getLastAccessTime()), tableInfo);
    formatOutput("Retention:", Integer.toString(tbl.getRetention()), tableInfo);
    if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
      String location = null;
      if (tbl.getSd() != null) {
        location = tbl.getSd().getLocation();
      }
      formatOutput("Location:", location, tableInfo);
    }
    formatOutput("Table Type:", tbl.getTableType(), tableInfo);

    if (tbl.getParameters().size() > 0) {
      tableInfo.append("Table Parameters:").append(LINE_DELIM);
      displayAllParameters(tbl.getParameters(), tableInfo, false, isOutputPadded);
    }
  }

  /**
   * The name of the statistic for Number of Erasure Coded Files - to be published or
   * gathered.
   */
  private static final String NUM_ERASURE_CODED_FILES = "numFilesErasureCoded";

  /**
   * Display key, value pairs of the parameters. The characters will be escaped including
   * unicode if escapeUnicode is true; otherwise the characters other than unicode will be
   * escaped.
   */
  private static void displayAllParameters(Map<String, String> params,
      StringBuilder tableInfo, boolean escapeUnicode, boolean isOutputPadded) {
    List<String> keys = new ArrayList<String>(params.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      String value = params.get(key);
      //TODO(Vihang) HIVE-18118 should be ported to Hive-3.1
      if (key.equals(NUM_ERASURE_CODED_FILES)) {
        if ("0".equals(value)) {
          continue;
        }
      }
      tableInfo.append(FIELD_DELIM); // Ensures all params are indented.
      formatOutput(key, escapeUnicode ? StringEscapeUtils.escapeJava(value)
          : ESCAPE_JAVA.translate(value), tableInfo, isOutputPadded);
    }
  }

  /**
   * Prints a row the given fields to a formatted line
   * @param fields The fields to print
   * @param tableInfo The target builder
   */
  private static void formatOutput(String[] fields, StringBuilder tableInfo) {
    formatOutput(fields, tableInfo, false, true);
  }

  /**
   * Prints the name value pair It the output is padded then unescape the value, so it
   * could be printed in multiple lines. In this case it assumes the pair is already
   * indented with a field delimiter
   *
   * @param name The field name to print
   * @param value The value t print
   * @param tableInfo The target builder
   * @param isOutputPadded Should the value printed as a padded string?
   */
  protected static void formatOutput(String name, String value, StringBuilder tableInfo,
      boolean isOutputPadded) {
    String unescapedValue = (isOutputPadded && value != null) ? value
        .replaceAll("\\\\n|\\\\r|\\\\r\\\\n", "\n") : value;
    formatOutput(name, unescapedValue, tableInfo);
  }

  /**
   * Prints the name value pair, and if the value contains newlines, it add one more empty
   * field before the two values (Assumes, the name value pair is already indented with
   * it)
   *
   * @param name The field name to print
   * @param value The value to print - might contain newlines
   * @param tableInfo The target builder
   */
  private static void formatOutput(String name, String value, StringBuilder tableInfo) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", name)).append(FIELD_DELIM);
    int colNameLength = ALIGNMENT > name.length() ? ALIGNMENT : name.length();
    indentMultilineValue(value, tableInfo, new int[]{0, colNameLength}, true);
  }


  private static String formatDate(long timeInSeconds) {
    if (timeInSeconds != 0) {
      Date date = new Date(timeInSeconds * 1000);
      return date.toString();
    }
    return "UNKNOWN";
  }

  private static void getStorageDescriptorInfo(StringBuilder tableInfo,
      StorageDescriptor storageDesc) {

    formatOutput("SerDe Library:", storageDesc.getSerdeInfo().getSerializationLib(),
        tableInfo);
    formatOutput("InputFormat:", storageDesc.getInputFormat(), tableInfo);
    formatOutput("OutputFormat:", storageDesc.getOutputFormat(), tableInfo);
    formatOutput("Compressed:", storageDesc.isCompressed() ? "Yes" : "No", tableInfo);
    formatOutput("Num Buckets:", String.valueOf(storageDesc.getNumBuckets()), tableInfo);
    formatOutput("Bucket Columns:", storageDesc.getBucketCols().toString(), tableInfo);
    formatOutput("Sort Columns:", storageDesc.getSortCols().toString(), tableInfo);
    if (storageDesc.isStoredAsSubDirectories()) {// optional parameter
      formatOutput("Stored As SubDirectories:", "Yes", tableInfo);
    }

    if (null != storageDesc.getSkewedInfo()) {
      List<String> skewedColNames =
          sortedList(storageDesc.getSkewedInfo().getSkewedColNames());
      if ((skewedColNames != null) && (skewedColNames.size() > 0)) {
        formatOutput("Skewed Columns:", skewedColNames.toString(), tableInfo);
      }

      List<List<String>> skewedColValues =
          sortedList(storageDesc.getSkewedInfo().getSkewedColValues(),
              new VectorComparator<String>());
      if ((skewedColValues != null) && (skewedColValues.size() > 0)) {
        formatOutput("Skewed Values:", skewedColValues.toString(), tableInfo);
      }

      Map<List<String>, String> skewedColMap =
          new TreeMap<>(new VectorComparator<>());
      skewedColMap.putAll(storageDesc.getSkewedInfo().getSkewedColValueLocationMaps());
      if ((skewedColMap != null) && (skewedColMap.size() > 0)) {
        formatOutput("Skewed Value to Path:", skewedColMap.toString(), tableInfo);
        Map<List<String>, String> truncatedSkewedColMap =
            new TreeMap<List<String>, String>(new VectorComparator<String>());
        // walk through existing map to truncate path so that test won't mask it
        // then we can verify location is right
        Set<Entry<List<String>, String>> entries = skewedColMap.entrySet();
        for (Entry<List<String>, String> entry : entries) {
          truncatedSkewedColMap.put(entry.getKey(), entry.getValue());
        }
        formatOutput("Skewed Value to Truncated Path:", truncatedSkewedColMap.toString(),
            tableInfo);
      }
    }

    if (storageDesc.getSerdeInfo().getParametersSize() > 0) {
      tableInfo.append("Storage Desc Params:").append(LINE_DELIM);
      displayAllParameters(storageDesc.getSerdeInfo().getParameters(), tableInfo, true,
          false);
    }
  }

  /**
   * Returns a sorted version of the given list, using the provided comparator
   */
  static <T> List<T> sortedList(List<T> list, Comparator<T> comp) {
    if (list == null || list.size() <= 1) {
      return list;
    }
    ArrayList<T> ret = new ArrayList<>();
    ret.addAll(list);
    Collections.sort(ret, comp);
    return ret;
  }

  /**
   * Returns a sorted version of the given list
   */
  static <T extends Comparable<T>> List<T> sortedList(List<T> list) {
    if (list == null || list.size() <= 1) {
      return list;
    }
    ArrayList<T> ret = new ArrayList<>();
    ret.addAll(list);
    Collections.sort(ret);
    return ret;
  }

  /**
   * Compares to lists of object T as vectors
   *
   * @param <T> the base object type. Must be {@link Comparable}
   */
  private static class VectorComparator<T extends Comparable<T>> implements
      Comparator<List<T>> {

    @Override
    public int compare(List<T> listA, List<T> listB) {
      for (int i = 0; i < listA.size() && i < listB.size(); i++) {
        T valA = listA.get(i);
        T valB = listB.get(i);
        if (valA != null) {
          int ret = valA.compareTo(valB);
          if (ret != 0) {
            return ret;
          }
        } else {
          if (valB != null) {
            return -1;
          }
        }
      }
      return Integer.compare(listA.size(), listB.size());
    }
  }
}
