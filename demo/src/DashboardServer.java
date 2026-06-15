import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Properties;

public class DashboardServer {

    public static void main(String[] args)
            throws Exception {

        HttpServer server =
                HttpServer.create(
                        new InetSocketAddress(
                                8081
                        ),
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

        server.start();

        System.out.println(
                "Dashboard API running at 8081"
        );
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
                    "OK"
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
                    "OK"
            );

        }catch(Exception e){

            e.printStackTrace();
        }
    }

    private static void sendOrder(
            String value
    ) throws Exception{

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

        KafkaProducer<String,String>
                producer =
                new KafkaProducer<>(
                        props
                );

        producer.send(
                new ProducerRecord<>(
                        "orders",
                        value
                )
        );

        producer.close();
    }

    private static void response(
            HttpExchange exchange,
            String body
    ) throws Exception{

        exchange.sendResponseHeaders(
                200,
                body.length()
        );

        OutputStream os =
                exchange.getResponseBody();

        os.write(
                body.getBytes()
        );

        os.close();
    }
}