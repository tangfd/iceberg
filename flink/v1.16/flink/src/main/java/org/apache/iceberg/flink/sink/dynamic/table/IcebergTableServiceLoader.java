/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink.dynamic.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.dynamic.TableInfo;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IcebergTableService 加载程序
 *
 * @author TangFD 2023/03/16
 */
public class IcebergTableServiceLoader {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergTableServiceLoader.class);
    private static final Map<String, Table> TABLE_CACHE = new HashMap<>();

    private static IcebergTableService icebergTableService;

    public static Table loadTable(TableInfo tableInfo) {
        Table table = load().loadTable(tableInfo);
        Preconditions.checkArgument(table != null, "Iceberg table shouldn't be null");
        TABLE_CACHE.put(tableInfo.getTable(), table);
        return table;
    }

    /**
     * 加载已存在得表
     *
     * @param tableName 表名
     * @return {@link Table}
     */
    public static Table loadExistTable(String tableName) {
        Table table = load().loadExistTable(tableName);
        Preconditions.checkArgument(table != null, "Iceberg table shouldn't be null");
        TABLE_CACHE.put(tableName, table);
        return table;
    }

    public static Table loadExistTableWithCache(String tableName) {
        Table table = TABLE_CACHE.get(tableName);
        if (table != null) {
            return table;
        }

        synchronized (IcebergTableServiceLoader.class) {
            table = TABLE_CACHE.get(tableName);
            if (table != null) {
                return table;
            }

            table = loadExistTable(tableName);
            TABLE_CACHE.put(tableName, table);
        }

        return table;
    }

    public static TableLoader tableLoader(String tableName) {
        TableLoader tableLoader = load().tableLoader(tableName);
        Preconditions.checkArgument(tableLoader != null, "Iceberg TableLoader shouldn't be null");
        return tableLoader;
    }

    private static IcebergTableService load() {
        if (icebergTableService != null) {
            return icebergTableService;
        }

        synchronized (IcebergTableServiceLoader.class) {
            if (icebergTableService != null) {
                return icebergTableService;
            }

            return serviceLoader();
        }
    }

    private static IcebergTableService serviceLoader() {
        ServiceLoader<IcebergTableService> serviceLoader = ServiceLoader.load(IcebergTableService.class);
        List<IcebergTableService> serviceList = new ArrayList<>();
        LOG.info("---------------------------------------------\nIcebergTableService实现类\n");
        for (IcebergTableService tableService : serviceLoader) {
            serviceList.add(tableService);
            LOG.info(tableService.getClass().getName());
        }

        LOG.info("---------------------------------------------");
        if (serviceList.size() == 0) {
            throw new RuntimeException("请先实现IcebergTableService，以提供Table信息");
        }

        icebergTableService = serviceList.get(0);
        LOG.info("---------------------------------------------");
        LOG.info("生效的IcebergTableService类：{}", icebergTableService.getClass().getName());
        LOG.info("---------------------------------------------");
        return icebergTableService;
    }
}
