package bottleneckjob;

import java.io.FileOutputStream;


import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.template.AbstractFileOutputTask;

public class FileWriterN extends AbstractFileOutputTask {

	private RecordReader<FileRecordN> input;
	
	@Override
	public void invoke() throws Exception {
		
		int count = 0;
		
		while(this.input.hasNext()) {
			
			FileRecordN fileRecord = this.input.next();
			System.out.println("Writter reicieved Record " + fileRecord.getId());
			if((count % 10) == 0) {
				System.out.println("Written " + count + " files");
			}
			FileOutputStream fos;

			fos = new FileOutputStream("/data/marrus/OCR/OCRpdf_" + (fileRecord.getId()) + ".pdf");

			fos.write(fileRecord.getBuffer());
			fos.close();
			fos = null;
			fileRecord = null;
		}
		
		System.out.println("Writer finishes after " + count + " files");
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<FileRecordN>(this, FileRecordN.class);
	}


}
