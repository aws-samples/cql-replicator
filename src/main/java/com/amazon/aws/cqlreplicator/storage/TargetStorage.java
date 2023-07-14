/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;

import java.util.concurrent.ExecutionException;

public abstract class TargetStorage<O, R, B, S> {
    public abstract void tearDown();

    public abstract R execute(B b);

    public abstract boolean write(S s) throws ExecutionException, InterruptedException;

    public abstract void writeStats(O o);

    public abstract R readStats(O o);

    public abstract void delete(O o);
}
