package np.com.kailasneupane.app;

import np.com.kailasneupane.CascadingUtils.flow.WordCountFlow;

import java.util.Scanner;

/**
 * Created by kailasneupane on 6/30/18.
 */
public class Main {

    static Scanner input = new Scanner(System.in);

    public static void main(String[] args) {

        String userName = System.getProperty("user.name");

        String source_path = "hdfs:///user/" + userName + "/word_count_input/";
        String sink_path = "hdfs:///user/" + userName + "/word_count_output/";

        System.out.println("Are files placed at location \"" + source_path + "\"? Y/N");
        if ("Y".equalsIgnoreCase(input.nextLine())) {
            WordCountFlow.startWCFlowHadoop(source_path, sink_path);
        } else {
            System.out.println("Exiting !!!");
        }
    }
}
