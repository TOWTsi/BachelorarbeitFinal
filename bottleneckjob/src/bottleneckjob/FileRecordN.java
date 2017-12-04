package bottleneckjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.stratosphere.nephele.fs.FSDataInputStream;
import eu.stratosphere.nephele.fs.FileInputSplit;
import eu.stratosphere.nephele.fs.FileSystem;
import eu.stratosphere.nephele.types.Record;

public class FileRecordN implements Record {

	private static final long SIZE_THRESHOLD = 32*1024*1024; //32 MB
	private static final int READ_SIZE = 8192; //8 KB
	
	private byte [] buffer = null;
	private int id;
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	private class FileRecordInputStream extends InputStream {
		
		private final FileRecordN fileRecord;
		private int bytesReadFromStream;;
		
		public FileRecordInputStream(FileRecordN fileRecord) {
			this.fileRecord = fileRecord;
			this.bytesReadFromStream = 0;
		}
		
		@Override
		public int read() throws IOException {
			
			if(this.bytesReadFromStream >= this.fileRecord.buffer.length) {
				return -1;
			}
			
			return this.fileRecord.buffer[this.bytesReadFromStream++];
		}
		
		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			
			if(this.bytesReadFromStream >= this.fileRecord.buffer.length) {
				return -1;
			}
			
			len = Math.min(len, (this.fileRecord.buffer.length-this.bytesReadFromStream));
			System.arraycopy(this.fileRecord.buffer, this.bytesReadFromStream, b, off, len);
			this.bytesReadFromStream += len;
			
			return len;
		}
		
		@Override
		public int read(byte[] b) throws IOException {
			return read(b, 0, b.length);
		}
		
		@Override
		public void reset() {
			this.bytesReadFromStream = 0;
		}

		@Override
		public void close() {
			reset();
		}
		
		@Override
		public long skip(long n) {
			
			final int bytesToSkip = (int) Math.min(n, (this.fileRecord.buffer.length-this.bytesReadFromStream));
			this.bytesReadFromStream += bytesToSkip;
			
			return bytesToSkip;
		}
		
		@Override
		public int available() {
			
			return (this.fileRecord.buffer.length - this.bytesReadFromStream);
		}
	}
	
	private class FileRecordOutputStream extends OutputStream {
		
		private final FileRecordN fileRecord;
		
		public FileRecordOutputStream(FileRecordN fileRecord) {
			this.fileRecord = fileRecord;
		}

		@Override
		public void write(int b) throws IOException {
			
			increaseBuffer(1);
			this.fileRecord.buffer[this.fileRecord.buffer.length-1] = (byte) b;
		}
		
		@Override
		public void write(byte[] b) {
			write(b, 0, b.length);
		}
		
		@Override
		public void write(byte[] b, int off, int len) {
			
			increaseBuffer(len);
			System.arraycopy(b, off, this.fileRecord.buffer, this.fileRecord.buffer.length-len, len);
		}
		
		private void increaseBuffer(int size) {
			
			if(this.fileRecord.buffer == null) {
				this.fileRecord.buffer = new byte[size];
			} else {
				byte [] oldBuf = this.fileRecord.buffer;
				this.fileRecord.buffer = new byte[oldBuf.length + size];
				System.arraycopy(oldBuf, 0, this.fileRecord.buffer, 0, oldBuf.length);
			}
		}
	}
	
	public static FileRecordN fromSplit(FileInputSplit fileInputSplit) throws IOException {
		
		if(fileInputSplit.getLength() > SIZE_THRESHOLD) {
			throw new IOException(fileInputSplit.getPath() + " is too large to be processed  " +  fileInputSplit.getLength() + " <> " + SIZE_THRESHOLD);
		}
		
		final FileSystem fs = FileSystem.get(fileInputSplit.getPath().toUri());
		final FSDataInputStream fdis = fs.open(fileInputSplit.getPath());
		
		final int length = (int) fileInputSplit.getLength();
		final FileRecordN fileRecord = new FileRecordN(length);
		
		int totalBytesRead = 0;
		fdis.seek(0);
		int bytesRead = fdis.read(fileRecord.buffer, totalBytesRead, (int) length - totalBytesRead);
		while(bytesRead != -1) {
			totalBytesRead += bytesRead;
			
			bytesRead = fdis.read(fileRecord.buffer, totalBytesRead, (int) Math.min(READ_SIZE, length - totalBytesRead));
			
			if((length - totalBytesRead) == 0) {
				break;
			}
		}
		
		return fileRecord;
	}
	
	public static FileRecordN fromFile(File file) throws IOException {
		
		if(!file.exists()) {
			throw new IOException("File does not exist");
		}
		
		if(file.length() > SIZE_THRESHOLD) {
			throw new IOException(file + " is too large to be processed");
		}
		
		final int length = (int) file.length();
		
		final FileInputStream fis = new FileInputStream(file);
		final FileRecordN fileRecord = new FileRecordN(length);
		
		int totalBytesRead = 0;
		int bytesRead;
		while(true) {
			bytesRead = fis.read(fileRecord.buffer, totalBytesRead, (fileRecord.buffer.length-totalBytesRead));
			if(bytesRead == -1) {
				break;
			}
			totalBytesRead += bytesRead;
			
			if(totalBytesRead >= fileRecord.buffer.length) {
				break;
			}
		}
		fis.close();
		return fileRecord;
	}
	
	private FileRecordN(int size) {
		this.buffer = new byte[size];
	}
	
	public FileRecordN() {
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		
		//Read size of buffer to receive
		final int bufferSize = in.readInt();
		//Create buffer according to the size
		this.buffer = new byte[bufferSize];
		//Read buffer content
		in.readFully(this.buffer);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		//Write out the size of the buffer to transmit
		out.writeInt(this.buffer.length);
		//Write out the buffer content
		out.write(this.buffer);
	}

	public InputStream getInputStream() {
		
		return new FileRecordInputStream(this);
	}

	public OutputStream getOutputStream() {
		
		return new FileRecordOutputStream(this);
	}
	
	public byte[] getBuffer() {
		return this.buffer;
	}
	
	/*public static void printBuffer(byte [] buffer, int off, int len) {
		
		for(int i = off; i < (off+len); i++) {
			System.out.println(buffer[i]);
		}
	}*/
}
