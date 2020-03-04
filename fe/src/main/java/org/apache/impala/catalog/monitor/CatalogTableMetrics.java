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

package org.apache.impala.catalog.monitor;

import com.google.common.base.Function;
import org.apache.impala.catalog.Table;
import org.apache.impala.util.TopNCache;

import java.util.List;

/**
 * Class that monitors catalog table usage. Currently, it tracks,
 *  - the most frequently accessed tables (in terms of number of metadata operations)
 *  - the tables with the highest (estimated) memory requirements
 *  - the tables with the highest number of files
 *  - the tables with the longest table metadata loading time
 *
 *  This class is thread-safe.
 */
public final class CatalogTableMetrics {
  public final static CatalogTableMetrics INSTANCE = new CatalogTableMetrics();

  private final TopNCache<Table, Long> frequentlyAccessedTables_;

  private final TopNCache<Table, Long> largestTables_;

  private final TopNCache<Table, Long> highFileCountTables_;

  private final TopNCache<Table, Long> longMetadataLoadingTables_;

  private CatalogTableMetrics() {
    final int num_tables_tracked = Integer.getInteger(
        "org.apache.impala.catalog.CatalogUsageMonitor.NUM_TABLES_TRACKED", 25);
    final int num_loading_time_tables_tracked = Integer.getInteger(
        "org.apache.impala.catalog.CatalogUsageMonitor.NUM_LOADING_TIME_TABLES_TRACKED",
        100);
    frequentlyAccessedTables_ = new TopNCache<Table, Long>(new Function<Table, Long>() {
      @Override
      public Long apply(Table tbl) {
        return tbl.getMetadataOpsCount();
      }
    }, num_tables_tracked, true);

    largestTables_ = new TopNCache<Table, Long>(new Function<Table, Long>() {
      @Override
      public Long apply(Table tbl) {
        return tbl.getEstimatedMetadataSize();
      }
    }, num_tables_tracked, false);

    highFileCountTables_ = new TopNCache<Table, Long>(new Function<Table, Long>() {
      @Override
      public Long apply(Table tbl) {
        return tbl.getNumFiles();
      }
    }, num_tables_tracked, false);

    // sort by maximum loading time by default
    longMetadataLoadingTables_ = new TopNCache<Table, Long>(new Function<Table, Long>() {
      @Override
      public Long apply(Table tbl) {
        return tbl.getMaxTableLoadingTime();
      }
    }, num_loading_time_tables_tracked, false);
  }

  public void updateFrequentlyAccessedTables(Table tbl) {
    frequentlyAccessedTables_.putOrUpdate(tbl);
  }

  public void updateLargestTables(Table tbl) { largestTables_.putOrUpdate(tbl); }

  public void updateHighFileCountTables(Table tbl) {
    highFileCountTables_.putOrUpdate(tbl);
  }

  public void updateLongMetadataLoadingTables(Table tbl) {
    longMetadataLoadingTables_.putOrUpdate(tbl);
  }

  public void removeTable(Table tbl) {
    frequentlyAccessedTables_.remove(tbl);
    largestTables_.remove(tbl);
    highFileCountTables_.remove(tbl);
    longMetadataLoadingTables_.remove(tbl);
  }

  public List<Table> getFrequentlyAccessedTables() {
    return frequentlyAccessedTables_.listEntries();
  }

  public List<Table> getLargestTables() { return largestTables_.listEntries(); }

  public List<Table> getHighFileCountTables() {
    return highFileCountTables_.listEntries();
  }

  public List<Table> getLongMetadataLoadingTables() {
    return longMetadataLoadingTables_.listEntries();
  }
}