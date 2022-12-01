/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator;

public class PreFlightCheckException extends Exception {
    public PreFlightCheckException(String errorMessage) {
        super(errorMessage);
    }

}
