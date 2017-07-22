import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class BranchOperationServer {

	public static Branch.Processor<Branch.Iface> processor = null;
	public static BranchHandler branchHandler = null;
	static String branchName = null;
	static int portNumber = 0;
	static Branch.Client client = null;
	static int snapshot_Num = 1;
	
	public static void main(String[] args) {

		if (args.length != 2) {
			System.err.println("Please check the arguments Proper Usage : <branch-name> <port-number>");
			System.exit(1);
		}
		branchName = args[0].trim();
		portNumber = Integer.parseInt(args[1]);
		branchHandler = new BranchHandler();
		processor = new Branch.Processor<Branch.Iface>(branchHandler);

		Runnable simple = new Runnable() {
			public void run() {
				simple(processor, branchName, portNumber);
			}
		};
		new Thread(simple).start();
		
		while (true) {
			try {
					for (BranchID branch : BranchHandler.all_BranchIDs) {
						TTransport transport = new TSocket(branch.getIp(), branch.getPort());
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						client = new Branch.Client(protocol);
						Thread.sleep((RandomNumberGenerator.randInt(1, 5)) * 1000);
						System.out.print("Money ssend to "+branch.getName());
						
						Runnable client_simple = new Runnable() {
							public void run() {
								try {
										perform(client,snapshot_Num);
										Thread.sleep((RandomNumberGenerator.randInt(1, 5)) * 1000);
										snapshot_Num += 1;
									//}
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

							}
						};
						new Thread(client_simple).start();
										// transport.close();
					}
					
					
			} catch (TException | InterruptedException x) {
				System.err.println("TEXCeption or InterruptedException has occurred in BranchOperationServer class"+x.getMessage());
				System.exit(1);
			}
	}
	}
	/**
	 * Perform operation for calling the transfer money from the application where the money will be transferred to the branches which are present in the list
	 * @param client
	 * @param snapshot_Num_In
	 */
	protected static void perform(Branch.Client client, int snapshot_Num_In) {
		/**
		 * Check the list of the branches and the stop transfer code which can be used to implement the distributed banking application
		 */
		if(!BranchHandler.all_BranchIDs.isEmpty() && BranchHandler.stop_transfer == 1) {
			TransferMessage message = new TransferMessage();
			int randInt = RandomNumberGenerator.randInt(1, 5);
			message.setAmount((int) (BranchHandler.balance_Amount*(float)randInt/100));
			//message.setAmount(10);
			System.out.println(" Inside Transfer Money Now  !  "+message.getAmount());
			message.setAmountIsSet(true);
			message.orig_branchId = BranchHandler.branchIDSwatahacha;
			BranchHandler.balance_Amount = BranchHandler.balance_Amount - message.getAmount();
			System.out.println("Balance after money transfer "+BranchHandler.balance_Amount);
			try {
				client.transferMoney(message);
			} catch (TException e) {
				System.err.println("TEXCeption or InterruptedException has occurred in BranchOperationServer class"+e.getMessage());
				System.exit(1);
			} 
		}  
		/*else if(!BranchHandler.channelList.isEmpty()){
				try {
					for (BranchID branch : BranchHandler.getAll_BranchIDs()) {
						TTransport transport = new TSocket(branch.getIp(), branch.getPort());
						transport.open();
						TProtocol protocol = new TBinaryProtocol(transport);
						client = new Branch.Client(protocol);
						client.Marker(branch, snapshot_Num_In);
					}
					BranchHandler.channelList.clear();
					} catch (TException e) {
						
					e.printStackTrace();
				}
			}*/
		
	}

	/**
	 * The simple server is generated for the specified version of the clients
	 * 
	 * @param processor
	 * @param portNumber
	 */
	public static void simple(Branch.Processor<Branch.Iface> processor, String branchName, int portNumber) {
		try {
			TServerTransport serverTransport = new TServerSocket(portNumber);
			// Use this for a multithreaded server
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

			System.out.println("Starting the MultiThreaded Simple Server... portNumber > " + portNumber
					+ " branchName > " + branchName);
			BranchHandler.branchName = branchName;
			/*String inetAddress  = InetAddress.getLocalHost().getHostAddress().replace("/", "");
			FileProcessorSupport fileProcessorSupport = new FileProcessor("branches.txt","branches.txt");
			fileProcessorSupport.openForWriting();
			StringBuilder contentsTowWrite = new StringBuilder();
			contentsTowWrite.append(branchName+" "+inetAddress+" "+portNumber+System.getProperty("line.separator"));
			fileProcessorSupport.writeToFile(contentsTowWrite.toString());
			fileProcessorSupport.getFileHandleWriterandClose();
			*/server.serve();
			serverTransport.close();
		} catch (Exception e) {
			System.err.println("The exception has occurred while starting the server. Try using on other port");
			System.exit(1);
		}
	}

}
