/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsCounter {
  private final ConcurrentMap<String, AtomicInteger> map = new ConcurrentHashMap<>();

  public StatsCounter() {}

  public void incrementStat(String key) {
    var value = map.get(key);
    if (value == null) {
      value = new AtomicInteger(0);
      var old = map.putIfAbsent(key, value);
      if (old != null) {
        value = old;
      }
    }
    value.incrementAndGet();
  }

  public Integer getStat(String key) {
    var value = map.get(key);
    return (value == null) ? 0 : value.get();
  }

  public void resetStat(String key) {
    var value = map.get(key);
    if (value == null) {
      value = new AtomicInteger(0);
      var old = map.putIfAbsent(key, value);
      if (old != null) {
        value = old;
      }
    }
    map.get(key).set(0);
  }
}
