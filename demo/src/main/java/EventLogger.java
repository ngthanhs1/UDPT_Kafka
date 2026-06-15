import java.io.FileWriter;
import java.io.IOException;

public class EventLogger {

    private static final String FILE =
            "demo/web/data/events.log";

    public static synchronized void log(
            String event
    ){

        try(
                FileWriter fw =
                        new FileWriter(
                                FILE,
                                true
                        )
        ){

            fw.write(
                    event + "\n"
            );

        }catch(
                IOException e
        ){
            e.printStackTrace();
        }
    }
}