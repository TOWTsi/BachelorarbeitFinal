package bottleneckjob;

import java.io.FileOutputStream;


import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

public class FileWriter extends AbstractFileOutputTask {

	private RecordReader<FileRecord> input;
	
	@Override
	public void invoke() throws Exception {
		
		int count = 0;
		
		while(this.input.hasNext()) {
			
			final FileRecord fileRecord = this.input.next();
			
			if((count % 10) == 0) {
				System.out.println("Written " + count + " files");
			}
			FileOutputStream fos;

			fos = new FileOutputStream("/data/marrus/OCR/OCRpdf_" + (count++) + ".pdf");

			fos.write(fileRecord.getBuffer());
			fos.close();
		}
		
		System.out.println("Writer finishes after " + count + " files");
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<FileRecord>(this, FileRecord.class);
	}


}
