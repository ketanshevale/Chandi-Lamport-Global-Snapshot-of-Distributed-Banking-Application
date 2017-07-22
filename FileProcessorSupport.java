public interface FileProcessorSupport {
	/**
	 * This function has been added to perform the operation of opening a file
	 * @param fileNameIn - To get the name of the file in the path on which the operations has to be performed
	 */
	public void openFile();
	
	/**
	 * This function has been added to perform the operation of closing a file
	 * @param fileNameIn - To get the name of the file in the path on which the operations has to be performed
	 */
	public void closeFile();
	
	/**
	 * This function reads from the file when the File descriptor has been provided by the File processor using the Open file
	 * @return String that has been read from the file for this application it has been restricted to reading single line from the file.
	 */
	public String readFromFile();
	
	/**
	 * This function write to the file when the File descriptor has been provided by the File processor using the Open file
	 * @return String that has been read from the file for this application it has been restricted to reading single line from the file.
	 */
	
	public void writeToFile(String contentsTowWrite);
	
	/**
	 * This function reads from the file when the File descriptor has been provided by the File processor using the Open file
	 * @return String that has been read from the file for this application it has been restricted to reading single line from the file.
	 */
	
	public void getFileHandleWriterandClose();
	
	/**
	 * This function is used for writing the file and closing the file after completing operations.
	 */
	public void openForWriting();
}
