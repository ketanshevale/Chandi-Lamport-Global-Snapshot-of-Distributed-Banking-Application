import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Controller {

	static Branch.Client client = null;
	static int snapshot_Num = 1;
	static int snaphot_Num_Retrieve =1;
	/**
	 * Read operation which reads the branches.txt file and then puts the values of all the branches that has been read from the file and put it into the list
	 * @param fileProcessorSupport
	 * @return
	 */
	public static List<BranchID> readFromFile(FileProcessorSupport fileProcessorSupport) {

		String readLine;
		List<BranchID> listBranchID = new ArrayList<BranchID>();
		BranchID branchID_In = null;
		/**
		 * Open the file for operation
		 */
		fileProcessorSupport.openFile();
		while ((readLine = fileProcessorSupport.readFromFile()) != null) {
			System.out.println(readLine);
			if(readLine.isEmpty() || readLine.trim().equals("") || readLine.trim().equals("\n")) {
				
			} else {
				branchID_In = new BranchID();
				String tokens[] = readLine.split("\\s+");
				branchID_In.setName(tokens[0].trim().toLowerCase());
				branchID_In.setIp(tokens[1].trim());
				branchID_In.setPort(Integer.parseInt(tokens[2]));
				listBranchID.add(branchID_In);
			}
		}
		System.out.println(listBranchID.toString());
		return listBranchID;

	}
	static List<BranchID> branchIdList = new ArrayList<BranchID>();
	static int initial_Amount;
	
	public static void main(String[] args) {

		String file_InputName;
		

		if (args.length != 2) {
			System.err.println("Please provide the arguments properly <initial_amount> <filename>");
			System.exit(1);
		}
		initial_Amount = Integer.parseInt(args[0].trim());
		file_InputName = args[1].trim();
		FileProcessorSupport fileProcessorSupport = new FileProcessor(file_InputName, file_InputName);
		branchIdList = readFromFile(fileProcessorSupport);
		try {
			//TTransport transport;
			// String host , int port
			for(BranchID branch: branchIdList) {
				TTransport transport = new TSocket(branch.getIp(),branch.getPort());
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new Branch.Client(protocol);
				perform(client);
				//transport.close();
			}
		} catch (TException x) {
			System.err.println("TEXCeption has occurred in COntroller class"+x.getMessage());
			System.exit(1);
		}
		Runnable initRandomSnapshot = new Runnable() {
			public void run() {
				try {
					while(true) {
						BranchID branch = branchIdList.get(RandomNumberGenerator.randInt(0, branchIdList.size()-1));
						TTransport transport = new TSocket(branch.getIp(),branch.getPort());
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						client = new Branch.Client(protocol);
						Thread.sleep(5000);
						client.initSnapshot(snapshot_Num);
						snapshot_Num += 1;
						
					}
				} catch (TException | InterruptedException e) {
					System.err.println("TEXCeption or InterruptedException has occurred in COntroller class"+e.getMessage());
					System.exit(1);
			
				}
			}
		};
		new Thread(initRandomSnapshot).start();
		
		Runnable retrieveRandomSnapshot = new Runnable() {
			public void run() {
				try {
					Branch.Client client = null;
					
					while(true) {
						LocalSnapshot localSnapshot = null;
						Thread.sleep(20000);
						System.out.println("SnapShot for SnapShot Number : "+snaphot_Num_Retrieve);
						
						for(BranchID branch: branchIdList) {
							localSnapshot = new LocalSnapshot();
							TTransport transport = new TSocket(branch.getIp(),branch.getPort());
							transport.open();
							TProtocol protocol = new TBinaryProtocol(transport);
							client = new Branch.Client(protocol);
							//transport.close();
							localSnapshot = client.retrieveSnapshot(snaphot_Num_Retrieve);
							System.out.println(localSnapshot.toString());
						}
						System.out.println("====================================================================");
						
						snaphot_Num_Retrieve += 1;
						
					}
					
				} catch (TException | InterruptedException e) {
					System.err.println("TEXCeption or InterruptedException has occurred in COntroller class"+e.getMessage());
					System.exit(1);
				}
			}
		};
		
		new Thread(retrieveRandomSnapshot).start();
		
	}
	/**
	 * /**
	 * Perform operation for calling the transfer money from the application where the money will be transferred to the branches which are present in the list
	 * @param client
	 * @param snapshot_Num_In
	 */
	
	 protected static void perform(Branch.Client client2) {
		// Initialize the branches 
				try {
					int singleBranchAmount = initial_Amount/branchIdList.size();
					for(BranchID branch: branchIdList) {
						System.out.println("Initial Amount assigned to Branch "+branch.getName()+" is "+singleBranchAmount);
					}
					//BranchHandler.all_BranchIDs = branchIdList;
					client2.initBranch(singleBranchAmount, branchIdList);
				} catch (TException e) {
					System.err.println("TEXCeption or InterruptedException has occurred in COntroller class"+e.getMessage());
					System.exit(1);
				}
	}
}
