package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    //--------------------- IntAggHandler Class ----------------------//

    private class IntCountHandler extends AggregateHandler {
        @Override
        public void calculate(Field gbField, Field aggField) {
            aggResult.put(gbField, aggResult.getOrDefault(gbField, 0) + 1);
        }
    }

    private class IntSumHandler extends AggregateHandler {
        @Override
        public void calculate(Field gbField, Field aggField) {
            int val = ((IntField) aggField).getValue();
            aggResult.put(gbField, aggResult.getOrDefault(gbField, 0) + val);
        }
    }

    private class IntMaxHandler extends AggregateHandler {
        @Override
        public void calculate(Field gbField, Field aggField) {
            int val = ((IntField) aggField).getValue();
            aggResult.put(gbField, Math.max(aggResult.getOrDefault(gbField, Integer.MIN_VALUE), val));
        }
    }

    private class IntMinHandler extends AggregateHandler {
        @Override
        public void calculate(Field gbField, Field aggField) {
            int val = ((IntField) aggField).getValue();
            aggResult.put(gbField, Math.min(aggResult.getOrDefault(gbField, Integer.MAX_VALUE), val));
        }
    }

    private class IntAvgHandler extends AggregateHandler {
        Map<Field, Integer> sum = new HashMap<>();
        Map<Field, Integer> count = new HashMap<>();
        @Override
        public void calculate(Field gbField, Field aggField) {
            int val = ((IntField) aggField).getValue();
            sum.put(gbField, sum.getOrDefault(gbField, 0) + val);
            count.put(gbField, count.getOrDefault(gbField, 0) + 1);
            aggResult.put(gbField, sum.get(gbField) / count.get(gbField));
        }
    }

    //--------------------- IntAggHandler Class ----------------------//


    //------------------------- private fields --------------------------//

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private int afield;
    private AggregateHandler handler;
    private Map<Op, AggregateHandler> handlerFactory;
    private TupleDesc resTupleDesc;

    //------------------------- private fields --------------------------//


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.afield = afield;
        initHandlerFactory();
        handler = handlerFactory.get(what);
        if (handler == null) {
            throw new IllegalArgumentException("Not supported here");
        }
        if (gbfield == NO_GROUPING) {
            resTupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            resTupleDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }
    }

    private void initHandlerFactory() {
        handlerFactory = new HashMap<>();
        handlerFactory.put(Op.COUNT, new IntCountHandler());
        handlerFactory.put(Op.MIN, new IntMinHandler());
        handlerFactory.put(Op.AVG, new IntAvgHandler());
        handlerFactory.put(Op.SUM, new IntSumHandler());
        handlerFactory.put(Op.MAX, new IntMaxHandler());
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField aggField = (IntField) tup.getField(afield);
        Field gbField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        handler.calculate(gbField, aggField);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
