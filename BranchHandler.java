import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

class ChannelState_Status {

	public LocalSnapshot localSnapshot = null;

	public enum Insert {
		NONRECORD, RECORD
	};

	//Integer record ;
	public volatile ConcurrentHashMap<String, Insert> channelState_Status = null;

	public ChannelState_Status() {
		localSnapshot = new LocalSnapshot();
		channelState_Status = new ConcurrentHashMap<String, Insert>();
	}
	
	public void intitalizeRecording(List<BranchID> allBranches) {
		for(BranchID branch : allBranches) {
			channelState_Status.put(branch.getName(),ChannelState_Status.Insert.RECORD);
		}
	}

	public LocalSnapshot getLocalSnapshot() {
		return localSnapshot;
	}

	public void setLocalSnapshot(LocalSnapshot localSnapshot) {
		this.localSnapshot = localSnapshot;
	}

	@Override
	public String toString() {
		return "ChannelState_Status [localSnapshot=" + localSnapshot + ", channelStatusMap=" + channelState_Status + "]";
	}

	public ConcurrentHashMap<String, Insert> getChannelStatusMap() {
		return channelState_Status;
	}

	public void setChannelStatusMap(ConcurrentHashMap<String, Insert> channelStatusMap) {
		this.channelState_Status = channelStatusMap;
	}
	
	
}

public class BranchHandler implements Branch.Iface {

	public static volatile int balance_Amount = 0;
	public static List<BranchID> all_BranchIDs = new ArrayList<BranchID>();
	public static String branchName;
	public static ConcurrentHashMap<Integer, ChannelState_Status> messageKeyValue = new ConcurrentHashMap<Integer, ChannelState_Status>();
	public static int count = 0;
	public static BranchID branchIDSwatahacha = new BranchID();
	public static volatile int stop_transfer = 1;

	/**
	 * initBranch this method has two input parameters: the initial balance of a branch and a list of all branches in thedistributed bank. 
	 * Upon receving this method, a branch will set its initial balance and record the list of all branches.
	 */
	@Override
	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {
		BranchHandler.balance_Amount = (balance);

		List<BranchID> branchListWithoutSwatah = new ArrayList<BranchID>();
		if (all_branches != null) {
			for (BranchID singleBranch : all_branches) {
				if (BranchHandler.branchName.equals(singleBranch.getName())) {
					branchIDSwatahacha.setIp(singleBranch.getIp());
					branchIDSwatahacha.setPort(singleBranch.getPort());
					branchIDSwatahacha.setName(singleBranch.getName());
				} else {
					branchListWithoutSwatah.add(singleBranch);
				}
			}
		}
		BranchHandler.setAll_BranchIDs(branchListWithoutSwatah);
		System.out.println("Insitail Balance :"+BranchHandler.getBalance_Amount());
		System.out.println("Branch List Without ME :"+BranchHandler.getAll_BranchIDs());
	}
	
	/**
	 * transferMoney given a TransferMessage structure that contains 
	 * the sending BranchID as well as the amount of money, the receiving branch updates its balance accordingly.
	 */
	@Override
	public void transferMoney(TransferMessage message) throws SystemException, TException {
		

		System.out.println("Initial Balance "+BranchHandler.balance_Amount);
		System.out.println("Money Has Come from :"+message.orig_branchId.getName()+"==="+ message.getAmount());
		
		BranchHandler.balance_Amount += message.getAmount();
		System.out.println("Updated Current Balance is :" + BranchHandler.balance_Amount);
		
		
		for (Map.Entry<Integer, ChannelState_Status> entryValue : messageKeyValue.entrySet()) {
		
			ChannelState_Status newChannelStatus = entryValue.getValue();
			int Key_SnapNumber = entryValue.getKey();
			System.out.println("newChannelStatus.channelStatusMap.get(message.getOrig_branchId().getName())  "+newChannelStatus.channelState_Status.get(message.getOrig_branchId().getName()));
			if( newChannelStatus.channelState_Status.get(message.getOrig_branchId().getName()) == ChannelState_Status.Insert.RECORD) {
				newChannelStatus.localSnapshot.messages.add(message.getAmount());
				messageKeyValue.put(Key_SnapNumber, newChannelStatus);
			}
			
		}
		

	}
	
	
	private void sendMarkerMessageesToAll(int snapshot_num) {
		// TODO Auto-generated method stub
		for (BranchID branch : all_BranchIDs) {
			TTransport transport = new TSocket(branch.getIp(), branch.getPort());
			try {
				transport.open();
			} catch (TTransportException e1) {
				System.err.println("TEXCeption or InterruptedException has occurred in TTransportException class"+e1.getMessage());
				System.exit(1);
			}
			TProtocol protocol = new TBinaryProtocol(transport);
			Branch.Client client = new Branch.Client(protocol);
				System.out.println("MarKer Send to "+branch.getName() +" for snap number "+snapshot_num);
				try {
					client.Marker(branchIDSwatahacha, snapshot_num);
				} catch (TException e) {
					System.err.println("TEXCeption or InterruptedException has occurred in TTransportException class"+e.getMessage());
					System.exit(1);
				
				}
			
		}
		
	}


	/**
	 * initSnapshot upon receiving this call, a branch records its own local state (balance) and sends out marker messages
	 * to all other branches by calling the Marker method on them. To identify multiple snapshots,
	 * the controller passes in a snapshot_num to this call, and all the marker messages should include this snapshot_num.
	 */
	@Override
	public void initSnapshot(int snapshot_num) throws SystemException, TException {
		BranchHandler.stop_transfer = 0;
		System.out.println("Transfer STopped Init MEthod");
		LocalSnapshot localSnapshot = new LocalSnapshot();

		localSnapshot.setBalance(BranchHandler.balance_Amount);
		localSnapshot.setSnapshot_num(snapshot_num);
		localSnapshot.setMessages(new CopyOnWriteArrayList<Integer>());

		ChannelState_Status channelState_Status = new ChannelState_Status();
		System.out.println("Snapshot Intiitated "+branchIDSwatahacha.getName()+" for snap number "+snapshot_num);
		channelState_Status.localSnapshot = localSnapshot;
		channelState_Status.intitalizeRecording(all_BranchIDs);
		messageKeyValue.put(snapshot_num, channelState_Status);
		
		sendMarkerMessageesToAll(snapshot_num);
		//System.out.println("OUTSIDE FOR LOOOP GOT HERE");
				BranchHandler.stop_transfer = 1;
				System.out.println("Transfer Started Init MEthod");
		//System.out.println("SnapShot Initiated " + branchIDSwatahacha.getName());
		
		// channelList.add(channelState_Status);

	}
	
	/**
	 * given the sending BranchID and snapshot_num, the receiving branch does the following:
	 */
	@Override
	public void Marker(BranchID branchId, int snapshot_num) throws SystemException, TException {
		System.out.println("Marker has come from " + branchId.getName()+" for snap number "+snapshot_num);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			System.err.println("InterruptedException has occurred in TTransportException class"+e.getMessage());
			System.exit(1);
		
		}
		if (BranchHandler.messageKeyValue.containsKey(snapshot_num)) {
			System.out.println("INSIDE IF :");
			ChannelState_Status channelState_Status = BranchHandler.messageKeyValue.get(snapshot_num);
			channelState_Status.channelState_Status.put(branchId.getName(),ChannelState_Status.Insert.NONRECORD);
			messageKeyValue.put(snapshot_num, channelState_Status);
			ChannelState_Status channelState_Status_faltu = BranchHandler.messageKeyValue.get(snapshot_num);	
			System.out.println(channelState_Status_faltu.channelState_Status.get(branchId.getName()));
			
		} else {
			BranchHandler.stop_transfer = 0;
			System.out.println("Transfer STopped Marker MEthod");
			LocalSnapshot localSnapshot = new LocalSnapshot();

			localSnapshot.setBalance(BranchHandler.balance_Amount);
			localSnapshot.setSnapshot_num(snapshot_num);
			localSnapshot.setMessages(new CopyOnWriteArrayList<Integer>());

			ChannelState_Status channelState_Status = new ChannelState_Status();
			channelState_Status.intitalizeRecording(all_BranchIDs);
			channelState_Status.localSnapshot = localSnapshot;
			channelState_Status.channelState_Status.put(branchId.getName(),ChannelState_Status.Insert.NONRECORD);
			messageKeyValue.put(snapshot_num, channelState_Status);
			
			sendMarkerMessageesToAll(snapshot_num);
			BranchHandler.stop_transfer = 1;
			System.out.println("Transfer STarted Marker MEthod");
		}
	}
	
	/**
	 * given the snapshot_num that uniquely identifies a snapshot, a branch retrieves its recorded 
	 * local and channel states and return them to the caller (i.e., the controller).
	 */
	@Override
	public LocalSnapshot retrieveSnapshot(int snapshot_num) throws SystemException, TException {
		System.out.println("retrieveSnapshot-------- " + snapshot_num);
		ChannelState_Status channelState_Status = messageKeyValue.get(snapshot_num);
		return channelState_Status.localSnapshot;
	}

	/**
	 * Getter and setter for all the data members
	 * @return
	 */
	public static  int getBalance_Amount() {
		return balance_Amount;
	}

	public static  void setBalance_Amount(int balance_Amount) {
		BranchHandler.balance_Amount = balance_Amount;
	}

	public static List<BranchID> getAll_BranchIDs() {
		return all_BranchIDs;
	}

	public static void setAll_BranchIDs(List<BranchID> all_BranchIDs) {
		BranchHandler.all_BranchIDs = all_BranchIDs;
	}

	public static String getBranchName() {
		return branchName;
	}

	public static void setBranchName(String branchName) {
		BranchHandler.branchName = branchName;
	}

}
