package bottleneckjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;

public class IndexRecord implements Record {

	public String key;

	public List<String> documents = new ArrayList<String>();
	
	public String toString() {
		return key.toString();
	}

	@Override
	public void read(DataInput in) throws IOException {
		
		this.key = StringRecord.readString(in);
		
		int numberOfDocuments = in.readInt();
		
		for(int i = 0; i < numberOfDocuments; i++) {
			documents.add(StringRecord.readString(in));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		StringRecord.writeString(out, this.key);
		
		out.writeInt(documents.size());
		Iterator<String> it = documents.iterator();	
		
		while(it.hasNext()) {
			StringRecord.writeString(out, it.next());
		}
	}


	
}
