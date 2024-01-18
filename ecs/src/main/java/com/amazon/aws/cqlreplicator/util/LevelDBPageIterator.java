package com.amazon.aws.cqlreplicator.util;

import com.amazon.aws.cqlreplicator.models.PrimaryKey;
import com.google.common.collect.AbstractIterator;
import org.apache.commons.lang3.SerializationUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import java.util.ArrayList;
import java.util.List;

public class LevelDBPageIterator extends AbstractIterator<List<PrimaryKey>> implements AutoCloseable {
    private static int PAGE_SIZE;
    private final DBIterator iterator;
    private PrimaryKey resumeKey;
    private boolean endOfData;

    public LevelDBPageIterator(DB levelDBStoreRows, int PAGE_SIZE) {
        this.iterator = levelDBStoreRows.iterator();
        LevelDBPageIterator.PAGE_SIZE = PAGE_SIZE;
    }

    private List<PrimaryKey> getData(PrimaryKey startKey, int PAGE_SIZE) {
        List<PrimaryKey> result = new ArrayList<>();
        if (startKey != null) {
            iterator.seek(SerializationUtils.serialize(startKey));
        } else {
            iterator.seekToFirst();
            if (iterator.hasNext()) {
                var nextKey = iterator.peekNext();
                var key = (PrimaryKey) SerializationUtils.deserialize(nextKey.getKey());
                result.add(key);
            }
        }

        for (int i = 0; i < PAGE_SIZE; i++) {
            if (iterator.hasNext()) {
                var nextKey = iterator.peekNext();
                var key = (PrimaryKey) SerializationUtils.deserialize(nextKey.getKey());
                result.add(key);
                iterator.next();
            } else break;
        }
        return result;
    }

    @Override
    protected List<PrimaryKey> computeNext() {
        if (endOfData) {
            return endOfData();
        }
        var rows = getData(resumeKey, PAGE_SIZE);

        if (rows.isEmpty()) {
            return endOfData();
        } else if (rows.size() < PAGE_SIZE) {
            endOfData = true;
        } else {
            resumeKey = rows.get(rows.size() - 1);
        }
        return rows;
    }

    @Override
    public void close() throws Exception {
        iterator.close();
    }
}
