
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileProcessor implements FileProcessorSupport {

	private FileReader fileReader = null;
	private FileWriter fileWriter = null;
	private String fileName = null;
	private String outputFileName = null;
	private BufferedReader getSingleLineBufferedReader = null;
	private BufferedWriter bufferedWriter = null;
	private File file = null;
	//private LoggerDebug loggerDebug = LoggerDebug.getInstance();

	/**
	 * This constructor sets the name of the file which has been used to perform the operation's
	 * @param in_Filename
	 */
	
	public FileProcessor() {
		//loggerDebug.printToStdout(2, "FileProcessor  Constructor has been called.");
	}

	public FileProcessor(String in_Filename, String out_Filename) {
		this.fileName = in_Filename;
		this.outputFileName = out_Filename;
		//loggerDebug.printToStdout(2, "FileProcessor parametrized Constructor has been called.");
	}
		
	@Override
	public void openFile() {
		try {
			file = new File(this.getFileName());
			if (file.length() == 0) {
				System.err.println("File length is zero exiting from the Application. "+this.getClass());
				System.exit(1);
			}
			fileReader = new FileReader(getFileName());
			this.setGetSingleLineBufferedReader(new BufferedReader(fileReader));
		} catch (FileNotFoundException e) {
			System.err.println("File that has been supposed to work for File operation has not been found in "+this.getClass());
			System.exit(1);
		}
	}
	
	public void openForWriting() {
		//loggerDebug.printToStdout(3, "FileProcessor  openForWriting has been called.");
		try {
			file = new File(this.getOutputFileName());
			if (!file.exists()) {
				file.createNewFile();
			}
			fileWriter = new FileWriter(file.getAbsoluteFile(),true);
			this.setBufferedWriter(new BufferedWriter(fileWriter));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	@Override
	public synchronized String readFromFile() {
		String getString = null;
		try {
			getString = this.getGetSingleLineBufferedReader().readLine();
			
		} catch (IOException e) {
			System.err.println("IOException in "+this.getClass()+" has been occurred while reading the file from the file system.");
			System.exit(1);
		}
			return getString;
	}
 
	@Override
	public void writeToFile(String contentsTowWrite) {
		//loggerDebug.printToStdout(3, "FileProcessor  openForWriting has been called.");
		try {
			this.getBufferedWriter().write(contentsTowWrite);
		} catch (IOException e) {
			System.err.println("IOException in "+this.getClass()+" has been occurred while writing the file from the file system.");
			System.exit(1);
		}
		
	}

	@Override
	public void getFileHandleWriterandClose() {
		//loggerDebug.printToStdout(3, "FileProcessor  openForWriting has been called.");
		try {
			this.getBufferedWriter().close();
		} catch (IOException e) {
			System.err.println("IOException in "+this.getClass()+" has been occurred while writing the file from the file system.");
			System.exit(1);
		}
	}

	@Override
	public void closeFile() {
		//loggerDebug.printToStdout(3, "FileProcessor closeFile has been called.");
		//file.
		try {
			this.getGetSingleLineBufferedReader().close();
		} catch (IOException e) {
			System.err.println("IOException in "+this.getClass()+" has been occurred while writing the file from the file system.");
			System.exit(1);
		}
	}
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public BufferedReader getGetSingleLineBufferedReader() {
		return getSingleLineBufferedReader;
	}

	public void setGetSingleLineBufferedReader(
			BufferedReader getSingleLineBufferedReader) {
		this.getSingleLineBufferedReader = getSingleLineBufferedReader;
	}

	public BufferedWriter getBufferedWriter() {
		return bufferedWriter;
	}

	public void setBufferedWriter(BufferedWriter bufferedWriter) {
		this.bufferedWriter = bufferedWriter;
	}

	public String getOutputFileName() {
		return outputFileName;
	}

	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}

	
	
}
