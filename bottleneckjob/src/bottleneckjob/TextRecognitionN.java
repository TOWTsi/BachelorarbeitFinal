package bottleneckjob;

import java.awt.image.BufferedImage;
//import java.util.StringTokenizer;
import java.util.StringTokenizer;

import javax.imageio.ImageIO;

import com.asprise.util.ocr.OCR;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;


public class TextRecognitionN extends AbstractTask implements ChannelSelector<StringPairRecordN> {

	private RecordReader<FileRecordN> input;
	private RecordWriter<StringPairRecordN> toPDF;
	private RecordWriter<StringPairRecordN> toInvertedIndex;
	
	private int[][] availableOutputChannels = null;
	private int numFile = 0;
	
	@Override
	public void invoke() throws Exception {
		
		final OCR ocr = new OCR();
		
		while(this.input.hasNext()) {
			final FileRecordN fileRecord = this.input.next();
			numFile++;
			//Convert file to image
			final BufferedImage bufferedImage = ImageIO.read(fileRecord.getInputStream());

			final String ocrResult = ocr.recognizeCharacters(bufferedImage);
			//Generate filename for the ocr;
			final String filename = getUniqueFilename();
			final StringPairRecordN pdfInput = new StringPairRecordN(filename, ocrResult);
			pdfInput.setId(fileRecord.getId());
			if(ocrResult != null){
				System.out.println("ocrresult " + ocrResult.length()  + " filename " + filename + " nr " + this.numFile  );

			//Send the filename entire string to the PDF creator
			this.toPDF.emit(pdfInput);
			
		final StringTokenizer stringTokenizer = new StringTokenizer(ocrResult);
			while(stringTokenizer.hasMoreElements()) {
				final String word = stringTokenizer.nextToken();
				final StringPairRecordN invertexIndexEntry = new StringPairRecordN(word, filename);
				this.toInvertedIndex.emit(invertexIndexEntry);
			}
		}
		}
	}
	
	
	
	private String getUniqueFilename() {
		
		final int length = 32;
		final char[] alphabeth = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
		String filename = "";
		
		for(int i = 0; i < length; i++) {
			filename += alphabeth[(int) (Math.random()*alphabeth.length)];
		}
		
		return filename + ".pdf";
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<FileRecordN>(this, FileRecordN.class);
		this.toPDF = new RecordWriter<StringPairRecordN>(this,new DefaultChannelSelector<StringPairRecordN>());
		this.toInvertedIndex = new RecordWriter<StringPairRecordN>(this, new DefaultChannelSelector<StringPairRecordN>());
	}



	@Override
	public int[] selectChannels(StringPairRecordN record, int numberOfChannels) {
		
		//Lazy initialization
		if(this.availableOutputChannels == null) {
			System.out.println("Initalizing lazy at OCr");
			this.availableOutputChannels = new int[numberOfChannels][];
			
			for(int i = 0; i < numberOfChannels; i++) {
				this.availableOutputChannels[i] = new int[1];
				this.availableOutputChannels[i][0] = i;
			}
		}
		
		return this.availableOutputChannels[hashString(record.getFirst(), numberOfChannels)];
	}

	private int hashString(String word, int numberOfChannels) {
		
		if(word.length() == 0) {
			return 0;
		}
		
		return (int)(word.charAt(0) % numberOfChannels);
	}

	
}
