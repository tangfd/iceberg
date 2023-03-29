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

package org.apache.iceberg.flink.sink.dynamic;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;

/**
 * @author TangFD 2023/3/29
 */
public class CacheUtils {

    public static <K, V> Cache<K, V> createCache() {
        return createCache(500);
    }

    public static <K, V> Cache<K, V> createCache(long maximumSize) {
        return createCache(maximumSize, Duration.ofMinutes(10), Duration.ofMinutes(10));
    }

    public static <K, V> Cache<K, V> createCache(long maximumSize, Duration expireAfterWrite, Duration refreshAfterWrite) {
        return Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWrite)
                .refreshAfterWrite(refreshAfterWrite)
                .build();
    }
}
