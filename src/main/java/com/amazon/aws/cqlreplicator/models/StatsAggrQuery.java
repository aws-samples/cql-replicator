// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.aws.cqlreplicator.models;

/** Defines stats query */
public class StatsAggrQuery {
  private String description;
  private String partiQL;

  public StatsAggrQuery(final String description, final String partiQL) {
    this.description = description;
    this.partiQL = partiQL;
  }

  @Override
  public String toString() {
    return "StatsAggrQuery{"
        + "description='"
        + description
        + '\''
        + ", partiQL='"
        + partiQL
        + '\''
        + '}';
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getPartiQL() {
    return partiQL;
  }

  public void setPartiQL(String partiQL) {
    this.partiQL = partiQL;
  }
}
