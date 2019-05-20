import org.apache.thrift.TException;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
public class SuperNodeServerHandler implements SuperNode.Iface
{

        TreeMap< Integer,String> NodeDetails;
        boolean isLocked;
        String lockedval;
        static int m;
        ArrayList<String> Nodes;
        private Object lock1 ;
        private Object lock2 ;
        public SuperNodeServerHandler()
        {
            isLocked=false;
            NodeDetails = new TreeMap<Integer,String>();
             Nodes= new ArrayList<String>();
             lock1 = new Object();
             lock2 = new Object();
        }

        //hashing function
        public static String encryptThisString(String input)
	    {
	        try {
	            // getInstance() method is called with algorithm SHA-1
	            MessageDigest md = MessageDigest.getInstance("SHA-1");


	            byte[] messageDigest = md.digest(input.getBytes());


	            BigInteger no = new BigInteger(1, messageDigest);

	            // Convert message digest into hex value
	            String hashtext = no.toString((int)Math.pow(2,m));

	            // Add preceding 0s to make it 32 bit
	            while (hashtext.length() < (int)Math.pow(2,m)) {
	                hashtext = "0" + hashtext;
	            }

	            // return the HashText
	            return hashtext;
	        }

	        // For specifying wrong message digest algorithms
	        catch (NoSuchAlgorithmException e) {
	            throw new RuntimeException(e);
	        }
	    }
        //hashing function


        @Override public boolean ping() throws TException {
			    System.out.println("I got ping() superNode");
			    return true;
		    }

        @Override public String Join(String IP,String Port) throws TException {
          String s = IP+","+Port;
          //synchronized(lock1) {
          try{
            if(isLocked)
            return "NACK";
            lockedval=s;
            isLocked=true;
            String hashedVal = encryptThisString(s);
            long val = (long)hashedVal.hashCode()+Integer.MAX_VALUE+1;
            val=val%64;
            NodeDetails.put((int)val,s);
            String op=GetNode();
            //System.out.println((int)val+""+op);
            return Integer.toString((int)val)+","+op;
          }
          catch(Exception e)
          {
            e.printStackTrace();
          }

        //}

        return "hello";
        }
        @Override public String PostJoin(String IP,String Port)throws TException {
            try{
              //Read Config File for testcase
              FileReader config = new FileReader("./data/config");
              Properties configProperties = new Properties();
              configProperties.load(config);
              m = Integer.parseInt(configProperties.getProperty("bit"));
            }catch(Exception ex){
              System.out.println(ex.getMessage());
            }
            String s = IP+","+Port;
        //  synchronized(lock2) {
            System.out.println(lockedval+" "+s);
            if(lockedval.equals(s))
            {
              String hashedVal = encryptThisString(s);
              long val = (long)hashedVal.hashCode()+Integer.MAX_VALUE+1;
              int mod = (int)Math.pow(2,m);
              val=val%mod;
              //NodeDetails.put((int)val,s);
              s=s.replaceAll(",",":");
              //System.out.println(s);
              Nodes.add(Integer.toString((int)val)+","+ s);
              isLocked=false;
                return Integer.toString((int)val)+","+ s;
          //  }
            }
          //String s = IP+":"+Port;

          return "NACK";
        }
        @Override public String GetNode() throws TException {
          if(Nodes.size()==0)
          return "None";

          Random rand = new Random();

        // Generate random integers in range 0 to Nodes.size()-1
        int index = rand.nextInt(Nodes.size());
          return Nodes.get(index);
        }
        //random number generation
        @Override public String  find_predecessor(String id,String nodeId )
        {
          Integer key=NodeDetails.lowerKey(Integer.parseInt(id));
          String predecessor = "";
          if(key==null)
          {
            for(Map.Entry<Integer,String> entry : NodeDetails.entrySet())
               predecessor = entry.getKey()+","+entry.getValue().split(",")[0];
          }
          else
          {
            predecessor = key+","+NodeDetails.get(key).split(",")[0];
          }
          return predecessor;
        }
        @Override public String  find_successor(String id,String nodeId)
        {
          Integer key=NodeDetails.ceilingKey(Integer.parseInt(id));
          String successor = "";
          if(key==null)
          {
            for(Map.Entry<Integer,String> entry : NodeDetails.entrySet())
               {
                 successor = entry.getKey()+","+entry.getValue().split(",")[0];
                 break;
               }
          }
          else
          {
            successor = key+","+NodeDetails.get(key).split(",")[0];
          }
          return successor;
        }

}
