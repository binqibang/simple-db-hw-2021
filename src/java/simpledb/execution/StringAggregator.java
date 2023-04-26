package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private int afield; // not used for lab2
    private AggregateHandler handler;
    private TupleDesc resTupleDesc;

    private class StringCountHandler extends AggregateHandler {
        @Override
        public void calculate(Field gbField, Field aggField) {
            aggResult.put(gbField, aggResult.getOrDefault(gbField, 0) + 1);
        }
    }


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Not supported here");
        }
        handler = new StringCountHandler();
        if (gbfield == NO_GROUPING) {
            resTupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            resTupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Field aggField = tup.getField(afield);
        handler.calculate(gbField, aggField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        List<Tuple> resTuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : handler.aggResult.entrySet()) {
            Tuple t = new Tuple(resTupleDesc);
            int val = entry.getValue();
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(val));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(val));
            }
            resTuples.add(t);
        }
        return new TupleIterator(resTupleDesc, resTuples);
    }

}
