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

// Generated code
public class SuperNodeServer {
    public static  SuperNodeServerHandler handler;
    public static SuperNode.Processor processor;

    public static void main(String [] args) {
        try {
            handler = new SuperNodeServerHandler();
            processor = new SuperNode.Processor(handler);

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

    public static void simple(SuperNode.Processor processor) {
        try {
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(9096);
            TTransportFactory factory = new TFramedTransport.Factory();

            //Create service request handler
            SuperNodeServerHandler handler = new SuperNodeServerHandler();
            processor = new SuperNode.Processor(handler);

            //Set server arguments
            TServer.Args args = new TServer.Args(serverTransport);
            args.processor(processor);  //Set handler
            args.transportFactory(factory);  //Set FramedTransport (for performance)

            //Run server as a single thread
            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
            server.serve();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
