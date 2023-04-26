package simpledb.execution;

import simpledb.storage.Field;

import java.util.HashMap;
import java.util.Map;

public abstract class AggregateHandler {

    /**
     * Aggregated result of specified Field.
     */
    protected Map<Field, Integer> aggResult;

    public AggregateHandler() {
        this.aggResult = new HashMap<>();
    }

    /**
     * Complete the specified aggregation of integer field.
     * @param gbField Group By Field
     * @param aggField Aggregate Field
     */
    public abstract void calculate(Field gbField, Field aggField);

}
