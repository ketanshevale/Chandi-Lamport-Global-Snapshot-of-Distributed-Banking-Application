import java.util.concurrent.ThreadLocalRandom;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MyBranch {
	//variable declaration
	public static String BranchName = null;
	public static int port = 9090;
	
	public static BankHandler handler;
	public static Branch.Processor<BankHandler> processor;
	public static int startBalance = -1;
//public  int myBranchInitialBalance=-1;
	public static int messageId=0;
	static final Object mutexLock = new Object();
	public static int branchSnapNumber = 0;
	public static int myMessageId = 1;
	
	public static void main(String[] args) {
		
		handler = new BankHandler();
		processor = new Branch.Processor<BankHandler>(handler);
		BranchName = args[0];
		//BranchName = "branch1";
		port = Integer.valueOf(args[1]);	//cmd
		
		//thread running to serve as a server
        Runnable simple = new Runnable() { public void run() {simple(processor);}};      
          new Thread(simple).start();
          //thread running to transfer money
          Runnable threadTransfer = new Runnable() { public void run() {try {
			threadTransfer(processor);
		} catch (InterruptedException | TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}}};      
          new Thread(threadTransfer).start();
		
	}
	//server function
	public static void simple(Branch.Processor<BankHandler> processor){
        try {
	          TServerTransport serverTransport = new TServerSocket(port);
	          TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
	          System.out.println("Starting the server...");
	          server.serve();
	        } catch (Exception e) {
	          e.printStackTrace();
	        }	
	} 
	
	//money transfer function
	public static void threadTransfer(Branch.Processor<BankHandler> processor) throws InterruptedException, TTransportException, SystemException{
		while(true){
			if(startBalance == -1){
				startBalance = handler.myBranchBalance;
			}
			if(startBalance > 0){
				Thread.sleep(3000);
				//if snapshot is initiated
				if(handler.currentSnapNumber > branchSnapNumber)
					transferMarkerProcess();
				//otherwise
				else
				transferProcess();
			}
			Thread.sleep(500);			
		}
	}
	public static void transferProcess() throws InterruptedException, TTransportException{
		try {
			Thread.sleep(3000);
			int bit =0;
			//calculate random amount from 1% to 5%
			int x =ThreadLocalRandom.current().nextInt(1, 6);
			int transationAmnt = (x*startBalance)/100;
			
//System.out.println(startBalance);
//System.out.println(transationAmnt);
			if(transationAmnt >0)
				if(handler.myBranchBalance >= transationAmnt){
					int currentSize = handler.myList.size();					
					while(bit == 0){
						//sending to random branch
						int rand = ThreadLocalRandom.current().nextInt(0, currentSize);
						if(!(BranchName.equals(handler.myList.get(rand).name))){
							String selectedBranch = handler.myList.get(rand).name;
							synchronized (mutexLock) {
							handler.myBranchBalance = handler.myBranchBalance - transationAmnt;
							System.out.println("Sent TO " + selectedBranch + " ==> " + transationAmnt);
							System.out.println("Current Amount is ==> " + handler.myBranchBalance);
							System.out.println();
							}
//creaing message
							TransferMessage tMessage = new TransferMessage();
							tMessage.amount= transationAmnt;
							tMessage.orig_branchId=null;
							for(BranchID b : handler.myList){
								if(b.name.equals(BranchName))
								tMessage.orig_branchId= new BranchID(b.name, b.ip, b.port);								
							}
//transfering message by calling process from handler							
							TTransport transport;
							transport = new TSocket(handler.myList.get(rand).ip, handler.myList.get(rand).port);
							transport.open();
							TProtocol protocol = new  TBinaryProtocol(transport);
							Branch.Client client = new Branch.Client(protocol);
							client.transferMoney(tMessage, myMessageId);
							
							transport.close();
							bit=1;
						}
						Thread.sleep(ThreadLocalRandom.current().nextInt(0, 6)* 1000);
					}
					
//System.out.println(rand);
					
				}
		} catch (SystemException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
	}
	//function to call if snapshot initiated
	public static void transferMarkerProcess() throws InterruptedException, TTransportException, SystemException{
		branchSnapNumber = handler.currentSnapNumber;
		if(branchSnapNumber == handler.mySnap.snapshotId){
			if(handler.markerSent.isEmpty() && handler.markerReceived.isEmpty()){
				//record balance
				handler.mySnap.balance = handler.myBranchBalance;
			}
		}
		//sending marker to other branches
		for(BranchID b : handler.myList){
			if(!(b.name.equals(BranchName))){
				if(handler.markerReceived.contains(b.name)){
					if(branchSnapNumber == handler.currentSnapNumber){
					System.out.println("Sending marker to " + b.name);
					}
				}
				handler.markerSent.add(b.name);
			}
		}
		Thread.sleep(3000);
		//calling client marker process
		for(BranchID b : handler.myList){
			if(!(b.name.equals(BranchName))){
				handler.markerSent.add(b.name);
				TTransport transport;
				transport = new TSocket(b.ip, b.port);
				transport.open();
				TProtocol protocol = new  TBinaryProtocol(transport);
				Branch.Client client = new Branch.Client(protocol);
				try {
					client.Marker(new BranchID(b.name, b.ip, b.port), branchSnapNumber, myMessageId);
					
				} catch (TException e) {e.printStackTrace();}
				transport.close();
			}
		}
		
	}

}
