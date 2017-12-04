package bottleneckjob;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Iterator;


import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;


public class IndexOutputWriterN extends AbstractFileOutputTask {

	private RecordReader<IndexRecordN> input;
	
	@Override
	public void invoke() throws Exception {
		
		int i = 0;
		while(this.input.hasNext()) {
			
			IndexRecordN indexRecord = this.input.next();
			FileWriter fw = new FileWriter("/tmp/myindex_" + (i++) + ".txt");
			BufferedWriter bufferedWriter = new BufferedWriter(fw);
			
			final Iterator<String> it = indexRecord.documents.iterator();
			while(it.hasNext()) {
				bufferedWriter.write(it.next() + "\n");
			}
			
			bufferedWriter.close();
			fw.close();
		}
		
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<IndexRecordN>(this, IndexRecordN.class);
	}

}
