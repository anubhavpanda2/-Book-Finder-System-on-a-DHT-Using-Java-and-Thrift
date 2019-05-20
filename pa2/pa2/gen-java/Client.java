import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;
public class Client {
    public static void main(String [] args) {
        //Create client connect.
        try {
          //Read Config File for testcase
          FileReader config = new FileReader("./data/config");
          Properties configProperties = new Properties();
          configProperties.load(config);
          String superNodeIP = configProperties.getProperty("superNodeIP");
          String inputFileConfig = configProperties.getProperty("inputFile");
          String inputDir = configProperties.getProperty("inputDir");
          int superNodePort = Integer.parseInt(configProperties.getProperty("superNodePort"));
          //supernode call
          //TTransport  transport1 = new TSocket("localhost", 9096);
          TTransport  transport1 = new TSocket(superNodeIP, superNodePort);
          TProtocol protocol1 = new TBinaryProtocol(new TFramedTransport(transport1));
          transport1.open();
          SuperNode.Client client = new SuperNode.Client(protocol1);
          String[] response=(client.GetNode().split(",")[1]).split(":");
          transport1.close();
          System.out.println(response[0]+response[1]);
          //nodecall
            TTransport  transport = new TSocket(response[0],Integer.parseInt(response[1]));
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            transport.open();
            DHTNode.Client dclient = new DHTNode.Client(protocol);

            //dclient.Set("Julius Caesar","war");
            //System.out.println(dclient.Get("  "));
            //Set
            String inputFileName = inputDir+inputFileConfig;
            File inputFile = new File(inputFileName);
            BufferedReader br;

		        String line;
		        try{
		            br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
		            line = null;
                int cnt=1;
		            while((line = br.readLine())!=null){
			               String[] words = line.split(":");
                     String[] parts = dclient.Set(words[0],words[1]).split(":");
                     String path = parts[1].substring(3,parts[1].length());
				             System.out.println(cnt+":"+words[0]+" | "+words[1]);
                     System.out.println("Routing Path: "+path+"\n");
                     cnt++;
		            }
                System.out.println("Set Successful");
		       }catch(Exception ex){
			          System.out.println("Input File Not Found");
		       }

           //Get
           char ch='n';
           do{
             BufferedReader b = new BufferedReader(new InputStreamReader(System.in));
             System.out.println("\nEnter the Book Title that you want to search");
             String book_title = b.readLine();
             String[] parts = dclient.Get(book_title).split(":");
             System.out.println(parts[0]);
             if(parts.length==2){
               String path = parts[1].substring(3,parts[1].length());
               System.out.println("Routing Path: "+path+"\n");
             }
             System.out.println("\nDo you want to continue?\nPress 'y' to continue and anything else to exit");
             ch = (char)b.read();
           }while(ch=='y');
          transport.close();





            //Try to connect

//transport1.open();
	//client.ping();
  	//System.out.println(client.GetNode());



        } catch(Exception e) {
			System.out.println(e.getMessage());
        }

    }
}
