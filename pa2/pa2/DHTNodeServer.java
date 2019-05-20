/*import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;
// Generated code
public class DHTNodeServer {
    public static DHTNodeServerHandler handler;
    public static DHTNode.Iface.Processor processor;

    public static void main(String [] args) {
        try {
            handler = new DHTNodeServerHandler();
            processor = new DHTNode.Processor(handler);
            handler.superNodejoin();
            Runnable simple = new Runnable() {
                public void run() {

                    simple(processor);
                }
            };
            //  handler.join();
            new Thread(simple).start();

        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(DHTNode.Processor processor) {
        try {
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(9095);
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            //DHTNodeServerHandler handler = new DHTNodeServerHandler();
            //processor = new DHTNode.Processor(handler);

            //Set server arguments
            //TServer.Args args = new TServer.Args(serverTransport);
          //  args.processor(processor);  //Set handler
            //args.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as a single thread
          //  TServer server = new TSimpleServer(args);
          TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
            server.serve();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}*/
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;



/*
  Server class
  1. Receives input from Client
  2. Creates the main thread for Server and WorkerNode interaction
*/

public class DHTNodeServer {
    public static DHTNodeServerHandler handler;
    public static DHTNode.Processor processor;
    static String ip, port;
    public static void main(String [] args) {

        try {
            handler = new DHTNodeServerHandler();
            processor = new DHTNode.Processor(handler);
            handler.superNodejoin();
          //  handler.connectToSuperNode(ip, port);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(DHTNode.Processor processor) {
        try {
                // TServerTransport serverTransport = new TServerSocket(9092);
                TServerTransport serverTransport = new TServerSocket(9095);
                TTransportFactory factory = new TFramedTransport.Factory();
                // TServer.Args args = new TServer.Args(serverTransport);
                // args.processor(processor);
                // args.transportFactory(factory);
                //Server server = new TSimpleServer(args);
                TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println("Node already present in the DHT");
        }
    }
}
