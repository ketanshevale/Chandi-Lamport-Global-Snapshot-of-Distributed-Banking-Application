import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import java.util.concurrent.ThreadLocalRandom;

public class Controller {
//Variable declaration
	public static int myTotalBalance =0;
	public static String filename = "";
	public static List<BranchID> myList = new ArrayList<BranchID>();
	public static BranchID firstStruct;
	public static int myBranchBalance =-1;
	public static int myBranchCount=0;
	public static int myControllerGlobalSnap=1;
	
	
	public static void main(String[] args) throws SystemException, TException, InterruptedException {
//cmd
		myTotalBalance = Integer.valueOf(args[0]);
		filename = "./"+args[1];
		
		try {
			//file processing and assigning initial balance
			fileProcess();
			myBranchCount = myList.size();
			myBranchBalance = (myTotalBalance/myBranchCount);
			//Now we got initial balance of each branch
			for(BranchID branch : myList){			
		      TTransport transport;
		        transport = new TSocket(branch.ip, branch.port);
		        transport.open();
		      TProtocol protocol = new  TBinaryProtocol(transport);
		      Branch.Client client = new Branch.Client(protocol);
		      //calling initBranch to set initial branch balance
		      client.initBranch(myBranchBalance, myList);
		      transport.close();		      
				//System.out.println(branch.name);
			}
			Thread.sleep(8000);
			while(true){
				//caling snapshot on random branch
				int rand = ThreadLocalRandom.current().nextInt(0, myList.size());
				
				System.out.println("Snapshot called by	:	" +myList.get(rand).name);
				
				TTransport transport1;
				transport1 = new TSocket(myList.get(rand).ip, myList.get(rand).port);
				transport1.open();
				TProtocol protocol1 = new  TBinaryProtocol(transport1);
				Branch.Client client1 = new Branch.Client(protocol1);
				client1.initSnapshot(myControllerGlobalSnap);
				 Thread.sleep(myList.size() * 3000);
				transport1.close();
				 
//					for(BranchID bid : myList){
//						Thread.sleep(myList.size() * 5000);	
//						TTransport transport2; 
//						transport2 = new TSocket(bid.ip, bid.port);
//						transport2.open();	
//						TProtocol protocol2 = new TBinaryProtocol(transport2);
//						Branch.Client client2 = new Branch.Client(protocol2);
//						transport1.close();						
//						LocalSnapshot snapshot = client1.retrieveSnapshot(myControllerGlobalSnap);
						//Thread.sleep(myList.size() * 3000);
						//transport1.close();
//						if(snapshot != null){
//							System.out.println("Balance of "+bid.name+" " + snapshot.balance);
//						}
//					}
//
//					Thread.sleep(3000);
					myControllerGlobalSnap++;
						
			}
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	//function to process file
	public static void fileProcess() throws FileNotFoundException, IOException{
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
		    String line;
		    while ((line = br.readLine()) != null) {
		    	String[] firstSplitedLine = line.split(" ");
		    	firstStruct = new BranchID();
		    	firstStruct.name = firstSplitedLine[0];
		    	firstStruct.ip	 = firstSplitedLine[1];
		    	firstStruct.port = Integer.valueOf(firstSplitedLine[2]);
		    	myList.add(firstStruct);
		    }
		    br.close();
		}
	}
	
	

}
