package bottleneckjob;

import java.util.Iterator;

import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;

public class InvertedIndexTask extends AbstractTask {

	private RecordReader<StringPairRecord> input = null;
	private RecordWriter<IndexRecord> output = null;

	@Override
	public void invoke() throws Exception {
		
		InvertedIndex index = new InvertedIndex();
		while (input.hasNext()) {
			StringPairRecord record = input.next();
			index.put(record.getFirst(), record.getSecond());
		}
		
		System.out.println("Emitting index");
		
		Iterator<String> it = index.getAllKeys();
		int i = 0;
		int documents = 0;
		while(it.hasNext()) {
			
			final String key = it.next();
			final IndexRecord record = index.getByKey(key);
			documents += record.documents.size();
			this.output.emit(record);
			++i;
		}
		System.out.println("Emitted " + i + " keys with " + documents + " documents");
		
	}

	/*private void emitStatusReport(InvertedIndex index) throws IOException,
			InterruptedException {
		String statusMessage = String.format(
				"Number of distinct keywords in index: %d", index
						.getNoOfDistinctKeywords());
		output.emit(new StringRecord(statusMessage));
	}*/

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<StringPairRecord>(this,
				StringPairRecord.class);
		this.output = new RecordWriter<IndexRecord>(this,new DefaultChannelSelector<IndexRecord>());
	}
}
