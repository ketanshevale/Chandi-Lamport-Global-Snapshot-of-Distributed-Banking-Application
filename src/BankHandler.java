import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

public class BankHandler implements Branch.Iface{

	//variable declaration
	public  int myBranchBalance=-1;
	public List<BranchID> myList = new ArrayList<BranchID>();
	
	public List<String> markerSent = new ArrayList<String>();
	public List<String> markerReceived = new ArrayList<String>();
	public Map<String, Integer> inbetweemRecords = new HashMap<String, Integer>(); 
	int inBetween = 0;	
	int currentSnapNumber = 0;
	public LocalSnapshot mySnap = new LocalSnapshot();
	public int bit = 0;
	static final Object mutexLock = new Object();
	public Map<String, Integer> prevMessageId = new HashMap<String, Integer>(); 
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	//branch will set its initial balance and list of all branches in system
	@Override
	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {
		// TODO Auto-generated method stub
		synchronized(mutexLock){
		this.myBranchBalance = balance;
		this.myList = all_branches;
		for(BranchID b : this.myList){
			prevMessageId.put(b.name, 0);
		}
		}
	}
//message containing original name, amount and messageid, to transfer money
	@Override
	public void transferMoney(TransferMessage message, int messageId) throws SystemException, TException {
		// TODO Auto-generated method stub
		synchronized (mutexLock) {
			//process if previous message id from map is == current messageid - 1
			if(prevMessageId.get(message.orig_branchId.name) == (messageId -1)){
			this.myBranchBalance = this.myBranchBalance + message.amount;
			//If markser message sent but not yet received
			if(!(markerReceived.contains(message.orig_branchId.name))){
				if(markerSent.contains(message.orig_branchId.name)){
					//calculate in between amount since last marker
					if(inbetweemRecords.containsKey(message.orig_branchId.name)){
						inBetween = inbetweemRecords.get(message.orig_branchId.name);
					}
					inbetweemRecords.put(message.orig_branchId.name, inBetween + message.amount);
					System.out.println("ON Channel amount is, "+message.orig_branchId.name+" ==> "+ (inBetween + message.amount));
					inBetween=0;
				}
			}
			//print received amount
			System.out.println("Received FROM " + message.orig_branchId.name + " ==> " + message.amount);
			System.out.println("Current Amount is ==> " + this.myBranchBalance);
			System.out.println();
			prevMessageId.put(message.orig_branchId.name, messageId);
			} 
		}	
	}
//initialise snapshot by recording current balance of recording branch, and sending marker to other branches
	//with snapshot id
	@Override
	public void initSnapshot(int snapshotId) throws SystemException, TException {
		// TODO Auto-generated method stub 
		synchronized(mutexLock){
		this. currentSnapNumber = snapshotId;
		this.bit = 1;
		this.mySnap.balance=this.myBranchBalance;
		this.mySnap.snapshotId = snapshotId;
		this.mySnap.messages = new ArrayList<Integer>();
			
		}
	}
//if first marker, record initial state and start recoeding on channel
	//otherwise start recording on channel
	@Override
	public void Marker(BranchID branchId, int snapshotId, int messageId) throws SystemException, TException {
		// TODO Auto-generated method stub
		synchronized (mutexLock) {
			//process if previous message id from map is == current messageid - 1
			if(prevMessageId.get(branchId.name) == (messageId -1)){
			System.out.println("Marker Received from "+branchId.name);
			System.out.println();
			this. currentSnapNumber = snapshotId;
			if(this.currentSnapNumber == snapshotId){
				//if marker is sent and not received
				if(markerSent.contains(branchId.name)){
					if(!(markerReceived.contains(branchId.name))){
						if(inbetweemRecords.containsKey(branchId.name)){
							//add messages from inbetwwen transactions
							mySnap.messages.add(inbetweemRecords.get(branchId.name));
						}
						//else add 0 to message
						else
							mySnap.messages.add(0);
					}
				}
			}
			//put current message id
			prevMessageId.put(branchId.name, messageId);
		}	
		}
		}

	
	//call retrive snapshot, each branch will call this method
	@Override
	public LocalSnapshot retrieveSnapshot(int snapshotId) throws SystemException, TException {
		// TODO Auto-generated method stub
		//send local snapshot to the function in controller
			LocalSnapshot snap = new LocalSnapshot();
			snap.snapshotId = snapshotId;
			snap.balance = mySnap.balance;
			snap.messages = new ArrayList<Integer>();
			for(int amount: mySnap.messages)
				snap.messages.add(amount);						
			inbetweemRecords.clear();
			//destructor to set values to default
			markerReceived.clear();
			markerSent.clear();
			inbetweemRecords.clear();
			bit=0;
			mySnap = new LocalSnapshot();
			return snap;
	}



	
}
