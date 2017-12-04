package bottleneckjob;


	import eu.stratosphere.nephele.fs.FileInputSplit;
	//import eu.stratosphere.nephele.io.DefaultChannelSelector;
	import eu.stratosphere.nephele.io.RecordWriter;
	import eu.stratosphere.nephele.template.AbstractFileInputTask;
	import java.util.Iterator;


	public class ImageReader extends AbstractFileInputTask {

		private RecordWriter<FileRecordN> output;
		
		@Override
		public void invoke() {
		
			final Iterator<FileInputSplit> inputSplits = getFileInputSplits();
			int i = 0;
			//for(int j = 0; j < 5; j++) {
					while(inputSplits.hasNext()) {
					FileRecordN fileRecord;
					try {
						FileInputSplit split = inputSplits.next();
						System.out.println("Reading file " + split.getPath());
						fileRecord = FileRecordN.fromSplit(split);
						fileRecord.setId(i);
						this.output.emit(fileRecord);
						i++;
						fileRecord = null;
						//System.out.println("emitted  record " + i);
					} catch (Exception e) {
						e.printStackTrace();
				}				

					}
					
					System.out.println("File Reader emitted " + i + " records");
		//	}

		
			
		}

		@Override
		public void registerInputOutput() {
			
			
			this.output = new RecordWriter<FileRecordN>(this);
			
			//this.output = new RecordWriter<FileRecord>(this, FileRecord.class, new DefaultChannelSelector<FileRecord>());
		}

	}


