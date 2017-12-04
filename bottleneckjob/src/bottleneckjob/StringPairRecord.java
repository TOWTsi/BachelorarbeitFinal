package bottleneckjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;

public class StringPairRecord implements Record {

	private String first;
	private String second;

	public StringPairRecord(String first, String second) {
		this.first = first;
		this.second = second;
	}
	
	public StringPairRecord() {
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	@Override
	public void read(DataInput in) throws IOException {
		first = StringRecord.readString(in);
		second = StringRecord.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		StringRecord.writeString(out, first);
		StringRecord.writeString(out, second);
	}
}
