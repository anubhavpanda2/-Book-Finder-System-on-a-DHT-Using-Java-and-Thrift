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
import javafx.util.Pair;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;
public class DHTNodeServerHandler implements DHTNode.Iface
{
  static int m;
  String superNodeIPconfig;
  String superNodePortconfig;
  String dhtNodePortconfig;
  public class DHTNodeInfo
  {
      String successor;
      String predecessor;
      String predecessorIP;
      String nodeId;
      String nodeIP;
      String contactnodeId;
      String contactNodeIp;
      String successorIP;
      ArrayList<String[]> fingerTable = new ArrayList<String[]>();
      ArrayList<String[]> interval = new ArrayList<String[]>();
      HashMap<String,String> dictionary = new HashMap<>();
      TreeMap<String,String> pathMap = new TreeMap<>();
  }
  DHTNodeInfo dhtNodeInfo;
        public  DHTNodeServerHandler()
        {

        //  superNodejoin();
        }
        public void superNodejoin(){
          //System.out.println("here");
          try{
            //Read Config File for testcase
			      FileReader config = new FileReader("./data/config");
			      Properties configProperties = new Properties();
			      configProperties.load(config);
			      superNodeIPconfig = configProperties.getProperty("superNodeIP");
            superNodePortconfig = configProperties.getProperty("superNodePort");
            dhtNodePortconfig = configProperties.getProperty("dhtNodePort");
            m = Integer.parseInt(configProperties.getProperty("bit"));
          }catch(Exception etop){
            etop.printStackTrace();
          }

          TTransport  transport = new TSocket(superNodeIPconfig, Integer.parseInt(superNodePortconfig));
          TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
          SuperNode.Client client = new SuperNode.Client(protocol);
           dhtNodeInfo=new DHTNodeInfo();
          try{
            InetAddress ip;
            String port="";
              String hostname="";
              try {
                    ip = InetAddress.getLocalHost();
                    hostname = ip.getHostName();
                    //System.out.println(ip);
                    dhtNodeInfo.nodeIP=hostname;
              } catch (UnknownHostException e) {

                  System.out.println("Hostname not found");
              }

          transport.open();
          String response=client.Join(dhtNodeInfo.nodeIP,dhtNodePortconfig);
          //after join
          //System.out.println("Anubhav"+response);
          String[] parts = response.split(",");
          dhtNodeInfo.nodeId=parts[0];
          if(parts[1].contains("None"))
          {

            for(int i=1;i<=m;i++)
            {
              String[] temp=new String[3];
              temp[0]=(((int)Math.pow(2,i-1)+Integer.parseInt(dhtNodeInfo.nodeId))%(int)Math.pow(2,m))+"";
              temp[1]=dhtNodeInfo.nodeId;
              temp[2]= dhtNodeInfo.nodeIP;
              dhtNodeInfo.fingerTable.add(temp);
            }
            dhtNodeInfo.predecessor=dhtNodeInfo.nodeId;
            dhtNodeInfo.successor=dhtNodeInfo.nodeId;
            dhtNodeInfo.successorIP=dhtNodeInfo.nodeIP;
            dhtNodeInfo.predecessorIP=dhtNodeInfo.nodeIP;
            dhtNodeInfo.contactNodeIp="NA";
          }
          else if(parts[1].contains("NACK"))
          {
            return;
          }
          else
          {
            init_Finger_table(dhtNodeInfo.nodeId,parts[1],parts[2]);
            UpdateDHT();
          }
          //after join
          //System.out.println(response+"test1");
          client.PostJoin(dhtNodeInfo.nodeIP,dhtNodePortconfig);
          //System.out.println(response+"test2");
          transport.close();
          }
            catch(Exception e)
            {
                System.out.println("exception"+e.getMessage());
                transport.close();
            }
            try{
              System.out.println("Node: "+dhtNodeInfo.nodeId+" | "+dhtNodeInfo.nodeIP+"\nSuccessor: "+dhtNodeInfo.successor+" | "+dhtNodeInfo.successorIP+"\nPredecessor: "+dhtNodeInfo.predecessor+" | "+dhtNodeInfo.predecessorIP);
              for(int i=0;i<m;i++)
              {
                System.out.println(dhtNodeInfo.fingerTable.get(i)[0]+"\t"+dhtNodeInfo.fingerTable.get(i)[1]+"\t"+dhtNodeInfo.fingerTable.get(i)[2]);
              }
              System.out.println("");
            }
            catch(Exception e)
            {
              //System.out.println(dhtNodeInfo.fingerTable.size());
              e.printStackTrace();
              System.out.println("exception"+dhtNodeInfo.nodeId+" "+dhtNodeInfo.nodeIP);
            }
        }
        @Override
        public boolean ping() throws TException {
			    System.out.println("I got ping() DHT");
			    return true;
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
        //Anubhav
        @Override
        public String print_path(String ip,String targetIp)
        {
          String res1=" ";
          try{
            if(dhtNodeInfo.pathMap.size()==0){
              for(int i=0;i<dhtNodeInfo.fingerTable.size();i++){
                dhtNodeInfo.pathMap.put(dhtNodeInfo.fingerTable.get(i)[0],dhtNodeInfo.fingerTable.get(i)[2]);
              }
            }
          res1  +="->"+dhtNodeInfo.nodeId;
            if(dhtNodeInfo.nodeId.equals(targetIp))
              return res1;
              String res=dhtNodeInfo.pathMap.lowerKey(targetIp);
              if(res==null)
              {
                for(Map.Entry<String,String> entry : dhtNodeInfo.pathMap.entrySet())
                  ip=entry.getValue();
              }
               ip=dhtNodeInfo.pathMap.get(res);
              if(ip!=targetIp)
              {
                Pair < DHTNode.Client,TTransport>clientDetails=get_client(ip);
                clientDetails.getValue().open();
                res1+=clientDetails.getKey().print_path(ip,targetIp);
                clientDetails.getValue().close();
              }
          }
          catch(Exception e)
          {
            //e.printStackTrace();
          }
          return res1;
        }
        //Anubhav
        @Override
        public String Set(String Book_title,String Genre) throws TException {
           String hashedVal = encryptThisString(Book_title);
           long val = (long)hashedVal.hashCode()+Integer.MAX_VALUE+1;
           val=val%(int)Math.pow(2,m);
           String id = (int)val+"";
           String path="";
           //network call to supernode
           try{

             Pair < SuperNode.Client,TTransport> client=client(dhtNodeInfo.contactNodeIp);
             client.getValue().open();
             String[] successorIP= (client.getKey().find_successor(id,dhtNodeInfo.contactnodeId)).split(",");
             client.getValue().close();

             if(successorIP.equals(dhtNodeInfo.nodeIP))
              Set_Dictionary(Book_title,Genre);
             else
             {
               Pair < DHTNode.Client,TTransport>clientDetails=get_client(successorIP[1]);
               clientDetails.getValue().open();
               clientDetails.getKey().Set_Dictionary(Book_title,Genre);
               clientDetails.getValue().close();
             }
             //network call to successor to set book_title and genre
             try{
            path=print_path(dhtNodeInfo.nodeId,successorIP[0]);
             }
             catch(Exception e)
             {

             }


           }
           catch(Exception e)
           {
             e.printStackTrace();
           }
	        // String successor = find_successor(id,dhtNodeInfo.nodeId);

          return "Set Successful"+":"+path;
        }
        @Override
        public String Get_Dictionary(String Book_title)
        {
            if(dhtNodeInfo.dictionary.containsKey(Book_title))
              return dhtNodeInfo.dictionary.get(Book_title);
            return "NA";
        }

        @Override
        public String Set_Dictionary(String Book_title,String Genre)
        {
          System.out.println(Book_title+" | "+Genre);
          dhtNodeInfo.dictionary.put(Book_title,Genre);
          return "Done";
        }
        @Override
        public String Get(String Book_title) throws TException {
          String hashedVal = encryptThisString(Book_title);
          long val = (long)hashedVal.hashCode()+Integer.MAX_VALUE+1;
          val=val%(int)Math.pow(2,m);
          String id = (int)val+"";
          String genre="";
          //network call to supernode
          try{

            Pair < SuperNode.Client,TTransport> client=client(dhtNodeInfo.contactNodeIp);
            client.getValue().open();
            String[] successorIP= (client.getKey().find_successor(id,dhtNodeInfo.contactnodeId)).split(",");

            client.getValue().close();


            if(successorIP[1].equals(dhtNodeInfo.nodeIP))
            genre=Get_Dictionary(Book_title);
            else
            {
              //network call to successor to get genre from dictionary.
              Pair < DHTNode.Client,TTransport>clientDetails=get_client(successorIP[1]);
              clientDetails.getValue().open();
              genre=clientDetails.getKey().Get_Dictionary(Book_title);//
              clientDetails.getValue().close();
            }
              try{
                  if(!genre.equals("NA"))
                    genre+=":"+print_path(dhtNodeInfo.nodeId,successorIP[0]);
                  }
                catch(Exception e)
                {

                }

          }
          catch(Exception e)
          {
            e.printStackTrace();
          }
          return genre.equals("NA")?"Book not present in the system":genre;
        }

        public void UpdateDHT()
        {
          HashSet<String> set = new HashSet<>();
          int c=(int)Math.pow(2,m);
          for(int i=1;i<=m;i++)
          {
            int val=Integer.parseInt(dhtNodeInfo.nodeId);
            String[] p=null;
            try{

              Pair < SuperNode.Client,TTransport> client=client(dhtNodeInfo.contactNodeIp);
              client.getValue().open();
                p= (client.getKey().find_predecessor((c+val-(int)Math.pow(2,i-1))%c+"",dhtNodeInfo.contactnodeId)).split(",");
              client.getValue().close();
            }
            catch(Exception e)
            {
              e.printStackTrace();
            }
            //String[] p=find_predecessor((c+val-(int)Math.pow(2,i-1))%c+"",dhtNodeInfo.contactnodeId).split(",");//ipaddress;node id is missing
            if(p[0].equals(dhtNodeInfo.nodeId))//||set.contains(p[0]))
            {
              continue;
            }
            try{
              Pair < DHTNode.Client,TTransport>clientDetails=get_client(p[1]);
            clientDetails.getValue().open();
            clientDetails.getKey().UpdateFingerTable(dhtNodeInfo.nodeId,i-1,dhtNodeInfo.nodeIP);//doubt
            clientDetails.getValue().close();
            }
            catch(Exception e)
            {
              e.printStackTrace();
            }
            set.add(p[0]);
          }
        }
        @Override
        public void UpdateFingerTable(String id,int i,String nodeIP)
        {
         if(id.equals(dhtNodeInfo.nodeId))
          {
            return;
          }
          int tempid=Integer.parseInt(id);
          int right=Integer.parseInt(dhtNodeInfo.fingerTable.get(i)[1]);
          int left=Integer.parseInt(dhtNodeInfo.nodeId);
          //System.out.println("tid "+tempid+" right "+right+" left "+left);
          int c = (int)Math.pow(2,m);
          if(left==0)
          right=(int)Math.pow(2,m)-1;
          else if(left==right)
          right--;
          if((right>=left&&tempid>=left&&tempid<right)||(right<=left&&((tempid>=0&&tempid<=right)||(tempid>=left&&tempid<=c))))
          {
              //System.out.println("if part tid "+tempid+" right "+right+" left "+left);
             tempid=(Integer.parseInt(dhtNodeInfo.nodeId)+(int)Math.pow(2,i+1))%(int)Math.pow(2,m);
             String val = dhtNodeInfo.fingerTable.get(i)[0];
             String[] li=new String[]{val,id,nodeIP};
             //li.add(new String(){tempid+"",id,dhtNodeInfo.nodeIP});

              dhtNodeInfo.fingerTable.set(i,li);
              //network call
              try{

                Pair < SuperNode.Client,TTransport> client=client(dhtNodeInfo.contactNodeIp);
                client.getValue().open();
                  String []predecessorDetails= (client.getKey().find_predecessor(dhtNodeInfo.nodeId,dhtNodeInfo.contactnodeId)).split(",");
                  dhtNodeInfo.predecessorIP=predecessorDetails[1];
                  dhtNodeInfo.predecessor=predecessorDetails[0];
                client.getValue().close();
              }
              catch(Exception e)
              {
                e.printStackTrace();
              }
              try{
                System.out.println("Node: "+dhtNodeInfo.nodeId+" | "+dhtNodeInfo.nodeIP+"\nSuccessor: "+dhtNodeInfo.fingerTable.get(0)[1]+" | "+dhtNodeInfo.fingerTable.get(0)[2]+"\nPredecessor: "+dhtNodeInfo.predecessor+" | "+dhtNodeInfo.predecessorIP);
                for(int j=0;j<m;j++)
                {
                  System.out.println(dhtNodeInfo.fingerTable.get(j)[0]+"\t"+dhtNodeInfo.fingerTable.get(j)[1]+"\t"+dhtNodeInfo.fingerTable.get(j)[2]);
                }
                System.out.println("update done"+dhtNodeInfo.nodeId);
              }
              catch(Exception e)
              {
                e.printStackTrace();
              }
              if( dhtNodeInfo.predecessorIP.equals(nodeIP))
              return;
              //network done
              Pair < DHTNode.Client,TTransport>clientDetails=get_client(dhtNodeInfo.predecessorIP);
              try{
                clientDetails.getValue().open();
                clientDetails.getKey().UpdateFingerTable(id,i,nodeIP);
                clientDetails.getValue().close();
              }
             catch(Exception e)
             {
                  e.printStackTrace();
             }
          }

        }
        @Override
        public String find_predecessor(String id,String nodeId)
        {
          //System.out.println("id"+id+"nodeId"+nodeId);
          if(nodeId.equals(dhtNodeInfo.nodeId))
          return dhtNodeInfo.nodeId+","+dhtNodeInfo.nodeIP;
          //String tempNode = dhtNodeInfo.nodeId;
	  String tempNode = nodeId;
          int tempid=Integer.parseInt(id);

          //int right=Integer.parseInt(dhtNodeInfo.successor);
	  int right = Integer.parseInt(tempNode);
          int left=Integer.parseInt(tempNode);
          //String ip=dhtNodeInfo.nodeIP;
	  String ip = dhtNodeInfo.contactNodeIp;
	  int cnt=0;
          while(!((right<=left&&tempid>=left&&tempid<right)||(right>left&&tempid>=right&&tempid<left+Math.pow(2,m)))){
            //network done with temp dhtNodeInfo.nodeIP
	    //cnt++;
	    //System.out.println(cnt+":here");
	    //if(cnt==10)break;
            try
            {

              if(ip.equals(dhtNodeInfo.nodeIP))
              {
                  String[] Response= closest_preceding_finger(id,nodeId).split(",");
                  ip=Response[1];
                  //network done to get successor of tempNode
                  String info= get_successor().split(",")[0];
                   right=Integer.parseInt(info);
                  tempNode=Response[0];
                   left=Integer.parseInt(tempNode);
		   //System.out.println(cnt+":here1"+"............."+ip);
                  continue;
              }
	      //System.out.println(cnt+"here2");
                Pair < DHTNode.Client,TTransport>clientDetails=get_client(ip);//doubtful
              clientDetails.getValue().open();
                String[] Response= clientDetails.getKey().closest_preceding_finger(id,nodeId).split(",");
              clientDetails.getValue().close();
              ip=Response[1];
              //network done to get successor of tempNode
              clientDetails=get_client(Response[1]);
              clientDetails.getValue().open();
              String info= clientDetails.getKey().get_successor().split(",")[0];
              clientDetails.getValue().close();
               right=Integer.parseInt(info);
              tempNode=Response[0];
               left=Integer.parseInt(tempNode);
            }
            catch(Exception e)
            {
              e.printStackTrace();
            }

          }
          return tempNode+","+ip;
        //  return "";
        }
        @Override
        public String closest_preceding_finger(String id,String nodeId)
        {
         for(int i=m-1;i>=0;i--)
          {
            int tempid=Integer.parseInt(dhtNodeInfo.fingerTable.get(i)[1]);

            int right=Integer.parseInt(id);
            int left=Integer.parseInt(dhtNodeInfo.nodeId);
            if(((right<left&&tempid>left&&tempid<right)||(right>left&&tempid>right&&tempid<left+Math.pow(2,m))))
              {
                return tempid+","+dhtNodeInfo.fingerTable.get(i)[2];
              }
          }
          return dhtNodeInfo.nodeId+","+dhtNodeInfo.nodeIP;
          //return "";
        }
        @Override
        public String find_successor(String id,String nodeId)
        {
        //  if(res[1].equals(dhtNodeInfo.nodeIP))

          if(nodeId.equals(dhtNodeInfo.nodeId))
          return dhtNodeInfo.successor+","+dhtNodeInfo.successorIP;
            String[] res= find_predecessor(id,nodeId).split(",");

            Pair < DHTNode.Client,TTransport>clientDetails=get_client(res[1]);
            try{
              clientDetails.getValue().open();
                String Response= clientDetails.getKey().get_successor();
              clientDetails.getValue().close();
              return Response;
            }
            catch(Exception E)
            {
              E.printStackTrace();
            }


          return "";

        }
        @Override
        public String get_successor()
        {
          //System.out.println(dhtNodeInfo.successor+","+dhtNodeInfo.successorIP+"test");
            return dhtNodeInfo.successor+","+dhtNodeInfo.successorIP;
            //return "";
        }
        @Override
        public void set_predecessor(String val,String ip)
        {
            dhtNodeInfo.predecessor=val;
            dhtNodeInfo.predecessorIP=ip;
        }


        //local methods
        public void init_Finger_table(String id,String contactNodeId,String  contactNodeIP)
        {
          try
          {
	    dhtNodeInfo.contactNodeIp=contactNodeIP;
            dhtNodeInfo.contactnodeId=contactNodeId;
            String key=((Integer.parseInt(id)+1)%(int)Math.pow(2,m))+"";
            //network done using contactNodeIP
            /*Pair < DHTNode.Client,TTransport>clientDetails=get_client(contactNodeIP);
              clientDetails.getValue().open();
              System.out.println(key+contactNodeId+"beforeFindSuccessor");
                String[] Response= (clientDetails.getKey().find_successor(key,contactNodeId)).split(",");
              clientDetails.getValue().close();*/
              Pair < SuperNode.Client,TTransport> client=client(contactNodeIP);
              client.getValue().open();
              //System.out.println(key+contactNodeId+"beforeFindSuccessor");
                String[] Response= (client.getKey().find_successor(key,contactNodeId)).split(",");
              client.getValue().close();

            dhtNodeInfo.successor=  Response[0];//find_successor(key,contactNodeId); //may send ip
            dhtNodeInfo.successorIP=  Response[1];
            dhtNodeInfo.fingerTable.add(new String[]{key,dhtNodeInfo.successor, dhtNodeInfo.successorIP});
            //network done

          /*  clientDetails=get_client( dhtNodeInfo.successorIP);
              clientDetails.getValue().open();
                  String [] res=clientDetails.getKey().find_predecessor(dhtNodeInfo.successor,contactNodeId).split(",");//doubtful we can implement get predecessor here
              clientDetails.getValue().close();
              dhtNodeInfo.predecessor=res[0];
              dhtNodeInfo.predecessorIP=res[1];*/
              client=client( dhtNodeInfo.successorIP);
                  client.getValue().open();
                      String [] res=client.getKey().find_predecessor(dhtNodeInfo.nodeId,contactNodeId).split(",");//doubtful we can implement get predecessor here
                  client.getValue().close();
                  dhtNodeInfo.predecessor=res[0];
                  dhtNodeInfo.predecessorIP=res[1];

            Pair < DHTNode.Client,TTransport>clientDetails=get_client(dhtNodeInfo.successorIP);
              clientDetails.getValue().open();
              clientDetails.getKey().set_predecessor(dhtNodeInfo.nodeId,dhtNodeInfo.nodeIP);//doubtful we can implement get predecessor here//Successor's predecessor is not set anywhere
              clientDetails.getValue().close();
          //  dhtNodeInfo.predecessor=find_predecessor(dhtNodeInfo.successor,contactNodeId);
            //network Done using successorIp

            //updatePredecessor("successorIp",dhtNodeInfo.successor);


            for(int i=0;i<m-1;i++)
            {
                  int tempid=((Integer.parseInt(id)+(int)Math.pow(2,i+1))%(int)Math.pow(2,m));
                  int right=Integer.parseInt(dhtNodeInfo.fingerTable.get(i)[1]);
                  int left=Integer.parseInt(id);
                  //System.out.println("tid "+tempid+" left"+left+" right"+right);
                  String ipnew=dhtNodeInfo.fingerTable.get(i)[2];
                  //if((right>left&&tempid>=left&&tempid<right)||(right<left&&tempid>=right&&tempid<left+Math.pow(2,m)))
                  //{
                    //  dhtNodeInfo.fingerTable.add(new String[]{tempid+"",right+"",ipnew});
                  //}
                  //else
                  //{
                    //System.out.println("else part");
                      //network Done using contactNodeIP
                    /*  clientDetails=get_client(contactNodeIP);
                      clientDetails.getValue().open();
                      Response= clientDetails.getKey().find_successor(tempid+"",contactNodeId).split(",");
                      clientDetails.getValue().close();*/
                      client=client(contactNodeIP);
                        client.getValue().open();
                        String re=client.getKey().find_successor(tempid+"",contactNodeId);
                        //System.out.println(re+" "+tempid);
                        Response= re.split(",");
                        client.getValue().close();
                      //String retval=find_successor(tempid+"",contactNodeId);
                      String[] li=new String[]{tempid+"",Response[0],Response[1]};
                      dhtNodeInfo.fingerTable.add(li);
                  //}
            }
          }
          catch(Exception E)
          {
              E.printStackTrace();
          }

        }
        Pair < DHTNode.Client,TTransport> get_client(String ip)
        {
        String[] res=  ip.split(":");
        int port;
        if(res.length==1)
         port=Integer.parseInt(dhtNodePortconfig);
        else
        port=Integer.parseInt(res[1]);
        //System.out.println(res[0]+" "+res[1]);
          TTransport  transport = new TSocket(res[0],port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            DHTNode.Client dclient =new DHTNode.Client(protocol);
            return new  Pair < DHTNode.Client,TTransport>(dclient,transport);
        }
        Pair < SuperNode.Client,TTransport> client(String ip)
        {

        //System.out.println(res[0]+" "+res[1]);
          TTransport  transport = new TSocket(superNodeIPconfig,Integer.parseInt(superNodePortconfig));
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            SuperNode.Client dclient =new SuperNode.Client(protocol);
            return new  Pair < SuperNode.Client,TTransport>(dclient,transport);
        }


}
