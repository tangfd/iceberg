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
package org.apache.iceberg.flink.sink.dynamic;

import java.io.Serializable;
import org.apache.iceberg.io.TaskWriter;

/**
 * Factory to create {@link TaskWriter}
 *
 * @author TangFD 2023/03/16
 */
public interface DynamicTaskWriterFactory<T> extends Serializable {

    /**
     * Initialize a {@link TaskWriter} with given task id and attempt id.
     *
     * @param tableInfo 表信息
     * @param taskId    taskId
     * @param attemptId attemptId
     * @return a newly created task writer.
     */
    TaskWriter<T> create(TableInfo tableInfo, int taskId, int attemptId);
}
