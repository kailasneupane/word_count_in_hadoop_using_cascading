package np.com.kailasneupane.CascadingUtils.assembly;

import cascading.operation.Insert;
import cascading.pipe.*;
import cascading.tuple.Fields;
import np.com.kailasneupane.CascadingUtils.buffer.CounterBuffer;
import np.com.kailasneupane.CascadingUtils.function.RowSplitterFunction;

//import cascading.operation.aggregator.Count;


/**
 * Created by kailasneupane on 7/1/18.
 */
public class WordCountAssembly extends SubAssembly {

    public WordCountAssembly(Pipe wc_pipe) {

        //splitting each sentence to words in a row
        wc_pipe = new Each(wc_pipe, new Fields("line"), new RowSplitterFunction(), Fields.REPLACE);

        /*
        //method 1 for word count:
        //applying groupBy to group same words and count each grouped words using prebuilt Count class.
        //we need to use GroupBy before any buffer operations inside Every class.
        wc_pipe = new GroupBy(wc_pipe, new Fields("line"));
        wc_pipe = new Every(wc_pipe,new Fields("line"), new Count(new Fields("count")),new Fields("line","count"));
        */

        //method 2:
        //Inserting new Field "count" in each tuple and applying groupBy to group the same words.
        // Then applying custom buffer CounterBuffer to get count of each grouped words,
        wc_pipe = new Each(wc_pipe, new Insert(new Fields("count"), 1), Fields.ALL);
        wc_pipe = new GroupBy(wc_pipe, new Fields("line"));
        wc_pipe = new Every(wc_pipe, new Fields("line", "count"), new CounterBuffer(), Fields.REPLACE);

        setTails(wc_pipe);
    }
}