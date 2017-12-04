package bottleneckjob;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import com.lowagie.text.Document;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Rectangle;
import com.lowagie.text.pdf.PdfReader;
import com.lowagie.text.pdf.PdfSignatureAppearance;
import com.lowagie.text.pdf.PdfStamper;
import com.lowagie.text.pdf.PdfWriter;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.io.DefaultChannelSelector;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;


public class PDFCreatorN extends AbstractTask {

	private static final String PDF_AUTHOR = "Technische Universität Berlin";
	private static final String PDF_CREATOR = "Technische Universität Berlin";
	
	private final Set<String> invalidKeywords = new HashSet<String>();
	
	private RecordReader<StringPairRecordN> input;
	private RecordWriter<FileRecordN> output;
	
	@Override
	public void invoke() throws Exception {
		
		populateInvalidKeywords();
		
		/**
		 * Certificate and key generated according to
		 * http://www.akadia.com/services/ssh_test_certificate.html
		 * http://www.tutorials.de/linux-unix/191874-openssl-pkcs12-export.html
		 */
		String keystorePath = GlobalConfiguration.getString("keystore.path", "/home/marrus/bundle.p12");
		KeyStore ks = KeyStore.getInstance("pkcs12");
		
		ks.load(new FileInputStream(keystorePath), "daniel".toCharArray());
		String alias = (String)ks.aliases().nextElement();
		PrivateKey key = (PrivateKey)ks.getKey(alias, "daniel".toCharArray());
		Certificate[] chain = ks.getCertificateChain(alias);
		int n = 0;	
		while(this.input.hasNext()) {
			n++;

			final StringPairRecordN stringPairRecord = this.input.next();
			final Document document = new Document();
			final FileRecordN fileRecord = new FileRecordN();
			PdfWriter.getInstance(document, fileRecord.getOutputStream());
			//pdfWriter.setEncryption(null, null, PdfWriter.ALLOW_PRINTING, PdfWriter.STANDARD_ENCRYPTION_128);
			document.open();
			
			//Meta data
			document.addTitle("Bottleneck Job");
			document.addAuthor(PDF_AUTHOR);
			document.addCreator(PDF_CREATOR);
			document.addCreationDate();
			document.addProducer();
			String mostFrequentKeyword = findMostFrequentKeyword(stringPairRecord.getSecond());
			if(mostFrequentKeyword != null) {
				document.addKeywords(mostFrequentKeyword);
			}
			
			//Add content to PDF
			document.add(new Paragraph(stringPairRecord.getSecond()));
			document.close();
			
			//Create signature
			PdfReader pdfReader = new PdfReader(fileRecord.getBuffer());
			FileRecordN signedPDF = new FileRecordN();
			PdfStamper stamper = PdfStamper.createSignature(pdfReader, signedPDF.getOutputStream(), '\0');
			PdfSignatureAppearance sap = stamper.getSignatureAppearance();
			sap.setCrypto(key, chain, null, PdfSignatureAppearance.SELF_SIGNED);
			sap.setReason("I'm the author");
			sap.setLocation("Berlin");
			sap.setVisibleSignature(new Rectangle(100, 100, 200, 200), 1, null);
			stamper.close();
			signedPDF.setId(stringPairRecord.getId());
			//System.out.println("finished Record number " + n  );
			this.output.emit(signedPDF);
		}
		
		System.out.println("PDF creator finishes after " + n + " Records");
	}

	@Override
	public void registerInputOutput() {
		
		this.input = new RecordReader<StringPairRecordN>(this, StringPairRecordN.class);
		this.output = new RecordWriter<FileRecordN>(this, new DefaultChannelSelector<FileRecordN>());
		
	}
	
	private void populateInvalidKeywords() {
		
		this.invalidKeywords.add("I");
		this.invalidKeywords.add("IT");
		this.invalidKeywords.add("HE");
		this.invalidKeywords.add("SHE");
		this.invalidKeywords.add("THAT");
		this.invalidKeywords.add("===");
		this.invalidKeywords.add(".");
		this.invalidKeywords.add("1");
		this.invalidKeywords.add("AND");
		this.invalidKeywords.add("THE");
		this.invalidKeywords.add("ASPRISE");
		this.invalidKeywords.add("OF");
		this.invalidKeywords.add("YOUR");
		this.invalidKeywords.add("SHALL");
		this.invalidKeywords.add("MY");
		this.invalidKeywords.add("YOU");
		this.invalidKeywords.add("YOUR");
		this.invalidKeywords.add("NOT");
		this.invalidKeywords.add("ALL");
		this.invalidKeywords.add("TO");
		
	}

	private String findMostFrequentKeyword(String str) {
		
		final StringTokenizer st = new StringTokenizer(str, " ");
		final Map<String, Integer> frequencyMap = new HashMap<String, Integer>();
		
		String mostFrequentKeyword = null;
		int mostFrequenctKeywordOccurence = 0;
		
		while(st.hasMoreTokens()) {
			
			String s = st.nextToken();
			if(s.length() == 0) {
				continue;
			}
			
			if(this.invalidKeywords.contains(s.toUpperCase())) {
				continue;
			}
			
			Integer i = frequencyMap.get(s);
			if(i == null) {
				frequencyMap.put(s, new Integer(1));
				if(1 > mostFrequenctKeywordOccurence) {
					mostFrequenctKeywordOccurence = 1;
					mostFrequentKeyword = s;
				}
			} else {
				int j = i.intValue();
				++j;
				frequencyMap.put(s, new Integer(j));
				if(j > mostFrequenctKeywordOccurence) {
					mostFrequenctKeywordOccurence = j;
					mostFrequentKeyword = s;
				}
			}
		}
		
		return mostFrequentKeyword;
	}
}
