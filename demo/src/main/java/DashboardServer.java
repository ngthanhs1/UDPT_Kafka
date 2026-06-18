import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class DashboardServer {

    private static KafkaProducer<String,String> producer;

    public static void main(String[] args)
            throws Exception {

        initProducer();

        HttpServer server =
                HttpServer.create(
                        new InetSocketAddress(8081),
                        0
                );

        server.createContext(
                "/send-success",
                DashboardServer::sendSuccess
        );

        server.createContext(
                "/send-failed",
                DashboardServer::sendFailed
        );

        server.createContext(
                "/logs",
                DashboardServer::getLogs
        );

        server.start();

        System.out.println(
                "Dashboard API running at http://localhost:8081"
        );
    }

    private static void initProducer(){

        Properties props =
                new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        producer =
                new KafkaProducer<>(props);
    }

    private static void sendSuccess(
            HttpExchange exchange
    ){

        try{

            sendOrder(
                    "Order-SUCCESS"
            );

            response(
                    exchange,
                    "SUCCESS ORDER SENT"
            );

        }catch(Exception e){

            e.printStackTrace();
        }
    }

    private static void sendFailed(
            HttpExchange exchange
    ){

        try{

            sendOrder(
                    "Order-5"
            );

            response(
                    exchange,
                    "FAILED ORDER SENT"
            );

        }catch(Exception e){

            e.printStackTrace();
        }
    }

    private static void getLogs(
            HttpExchange exchange
    ){

        try{

            String content =
                    Files.readString(
                            Paths.get(
                                    "demo/web/data/events.log"
                            )
                    );

            response(
                    exchange,
                    content
            );

        }catch(Exception e){

            e.printStackTrace();
        }
    }

    private static void sendOrder(
            String value
    ) throws Exception{

        producer.send(
                new ProducerRecord<>(
                        "orders",
                        value
                )
        );

        producer.flush();

        System.out.println(
                "[DASHBOARD] Sent -> "
                        + value
        );
    }

    private static void response(
            HttpExchange exchange,
            String body
    ) throws Exception{

        exchange.getResponseHeaders().add(
                "Access-Control-Allow-Origin",
                "*"
        );

        exchange.sendResponseHeaders(
                200,
                body.getBytes().length
        );

        OutputStream os =
                exchange.getResponseBody();

        os.write(
                body.getBytes()
        );

        os.close();
    }
}
