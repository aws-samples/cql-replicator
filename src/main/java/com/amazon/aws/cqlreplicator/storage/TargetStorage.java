/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.aws.cqlreplicator.storage;


import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class TargetStorage<O, S> {

    public abstract void tearDown();

    public abstract boolean write(S s) throws ExecutionException, InterruptedException, IOException;

    public abstract void delete(O o) throws IOException;

    public abstract boolean write(S s, O o) throws IOException;

    public abstract void delete(S s, O o) throws IOException;
}
