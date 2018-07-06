package np.com.kailasneupane.CascadingUtils.flow;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import np.com.kailasneupane.CascadingUtils.assembly.WordCountAssembly;

/**
 * Created by kailasneupane on 7/1/18.
 */
public class WordCountFlow {
    private static Tap getSourceTap(String source_path) {
        return new Hfs(new TextLine(new Fields("line")), source_path);
    }

    private static Tap getSinkTap(String sink_path) {
        return new Hfs(new TextDelimited(new Fields("line", "count"), false, ";"), sink_path, SinkMode.REPLACE);
    }

    public static void startWCFlowHadoop(String source_path, String sink_path) {
        Pipe wc_pipe = new Pipe("WordCountPipe");
        wc_pipe = new WordCountAssembly(wc_pipe);
        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(wc_pipe, getSourceTap(source_path))
                .addTailSink(wc_pipe, getSinkTap(sink_path));
        new HadoopFlowConnector().connect(flowDef).complete();
        System.out.println("Process Completed. Please find output data at following location:\n" + sink_path);
    }
}