package bottleneckjob;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;


import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;


public class IndexOutputWriter extends AbstractFileOutputTask {

	private RecordReader<IndexRecord> input;
	
	@Override
	public void invoke() throws Exception {
		
		int i = 0;
		while(this.input.hasNext()) {
			
			IndexRecord indexRecord = this.input.next();
			
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/myindex_" + (i++) + ".txt"));
			
			final Iterator<String> it = indexRecord.documents.iterator();
			while(it.hasNext()) {
				bufferedWriter.write(it.next() + "\n");
			}
			
			bufferedWriter.close();
		}
		
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<IndexRecord>(this, IndexRecord.class);
	}

}
