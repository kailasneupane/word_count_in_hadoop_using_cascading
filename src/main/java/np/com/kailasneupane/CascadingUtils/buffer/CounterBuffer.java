package np.com.kailasneupane.CascadingUtils.buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by kailasneupane on 7/1/18.
 */
public class CounterBuffer extends BaseOperation implements Buffer {
    public CounterBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        ArrayList<TupleEntry> tupleEntries = new ArrayList<>();
        tupleEntries.add(new TupleEntry(tupleEntryIterator.next()));
        tupleEntries.get(0).setString("count", String.valueOf(getIteratorSize(tupleEntryIterator) + 1));
        //System.out.println(tupleEntries.get(0).getTuple());
        bufferCall.getOutputCollector().add(tupleEntries.get(0));
    }

    private long getIteratorSize(Iterator iterator) {
        Long i = 0L;
        while (iterator.hasNext()) {
            iterator.next();
            i++;
        }
        return i;
    }
}