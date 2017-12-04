package bottleneckjob;

import eu.stratosphere.nephele.fs.FileInputSplit;
//import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractFileInputTask;
import java.util.Iterator;

public class FileReader extends AbstractFileInputTask {

	private RecordWriter<FileRecord> output;
	
	@Override
	public void invoke() {
	
		final Iterator<FileInputSplit> inputSplits = getFileInputSplits();
		
		for(int j = 0; j < 5; j++) {
				while(inputSplits.hasNext()) {
				FileRecord fileRecord;
				try {
					fileRecord = FileRecord.fromSplit(inputSplits.next());
					this.output.emit(fileRecord);
				} catch (Exception e) {
					e.printStackTrace();
			}				

				}
		}

	
		
	}

	@Override
	public void registerInputOutput() {
		
		
		this.output = new RecordWriter<FileRecord>(this);
		
		//this.output = new RecordWriter<FileRecord>(this, FileRecord.class, new DefaultChannelSelector<FileRecord>());
	}

}
