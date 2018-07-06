package np.com.kailasneupane.CascadingUtils.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by kailasneupane on 6/30/18.
 */
public class RowSplitterFunction extends BaseOperation implements Function {

    public RowSplitterFunction() {
        super(Fields.ARGS);
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry tupleEntry = new TupleEntry(functionCall.getArguments());
        TupleEntry tuple = new TupleEntry(tupleEntry);

        String row = tuple.getString("line");
        //System.out.println(row);
        String[] words = row.split("\\s+");

        for (String word : words) {
            tuple.setString("line", word.toLowerCase());
            //System.out.println(word);
            functionCall.getOutputCollector().add(tuple);
        }

    }
}
