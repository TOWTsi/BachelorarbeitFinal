package bottleneckjob;

import java.io.IOException;

import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.fs.Path;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.JobFileInputVertex;
import eu.stratosphere.nephele.jobgraph.JobFileOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;

public class BottleneckJob123{

	public static void main(String [] args) throws JobExecutionException, InterruptedException  {
		
		if(args.length < 5){
			System.out.println("Usage: INPUTPATH OUTPUTPATH EXTERNALJARPATH JOBMANAGERIP JOBMANAERPORT");
			return;
		}
		JobGraph jobGraph = new JobGraph("BMP2PDF Job all CP");
		
		final int numberOfOCRTasks =12; //24
		final int numberOfPDFTasks = 3; //6
		final int numberOfIndexTasks = 1;
		final int numberOfFileReader = 1;
		
		JobFileInputVertex fileReader = new JobFileInputVertex("File Reader", jobGraph);
		fileReader.setFileInputClass(ImageReader.class);
		//fileReader.setUdf(FileReader.class.getName());

		fileReader.setFilePath(new Path(args[0]));
		//System.out.println(	fileReader.getFilePath());
		fileReader.setNumberOfSubtasks(numberOfFileReader);
		
		JobTaskVertex textRecognition = new JobTaskVertex("OCR Task", jobGraph);
		textRecognition.setTaskClass(TextRecognitionN.class);
		//textRecognition.setUdf(TextRecognition.class.getName());
		textRecognition.setNumberOfSubtasks(numberOfOCRTasks);
		textRecognition.setNumberOfSubtasksPerInstance(1);
		//textRecognition.setUpdated();
		
		
		JobTaskVertex pdfCreator = new JobTaskVertex("PDF Creator", jobGraph);
		pdfCreator.setTaskClass(PDFCreatorN.class);
		//pdfCreator.setUdf(PDFCreator.class.getName());
		pdfCreator.setNumberOfSubtasks(numberOfPDFTasks);
		//pdfCreator.setUpdated();
		
		JobFileOutputVertex fileWriter = new JobFileOutputVertex("File Writer", jobGraph);
		fileWriter.setFileOutputClass(FileWriterN.class);
		//fileWriter.setUdf(FileWriter.class.getName());
		fileWriter.setFilePath(new Path(args[1]));
		fileWriter.setNumberOfSubtasks(1);
		
		
		JobTaskVertex invertexIndex = new JobTaskVertex("Inverted Index", jobGraph);
		invertexIndex.setTaskClass(InvertedIndexTaskN.class);
		//invertexIndex.setUdf(InvertedIndexTask.class.getName());
		invertexIndex.setNumberOfSubtasks(numberOfIndexTasks);
		//invertexIndex.setUpdated();
		
		JobFileOutputVertex indexWriter = new JobFileOutputVertex("Index Writer", jobGraph);
		indexWriter.setFileOutputClass(IndexOutputWriterN.class);
		//indexWriter.setUdf(IndexOutputWriter.class.getName());
		indexWriter.setFilePath(new Path(args[1]));
		indexWriter.setNumberOfSubtasks(1);
		
		fileWriter.setVertexToShareInstancesWith(fileReader);
		indexWriter.setVertexToShareInstancesWith(fileReader);
		//invertexIndex.setVertexToShareInstancesWith(fileReader);

		try {
			fileReader.connectTo(textRecognition, ChannelType.NETWORK, null);
			textRecognition.connectTo(pdfCreator, ChannelType.NETWORK, null);
			textRecognition.connectTo(invertexIndex, ChannelType.NETWORK, null);
			pdfCreator.connectTo(fileWriter, ChannelType.NETWORK, null);
			invertexIndex.connectTo(indexWriter, ChannelType.NETWORK, null);
		} catch(JobGraphDefinitionException e) {
			e.printStackTrace();
			return;
		}
		System.out.println("Connected");
		//Add required jar files
		jobGraph.addJar(new Path("file://" +args[2] +"/bottle-12-3-1.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/OCR.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/aspriseOCR.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/iText-2.1.5.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/bcmail-jdk16-145.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/bcprov-jdk16-145.jar"));
		jobGraph.addJar(new Path("file://" +args[2] +"/bctsp-jdk16-145.jar"));	

		
		System.out.println("Added JARS");
		//Disable profiling
		jobGraph.getJobConfiguration().setBoolean("job.profiling.enable", false);
		 
		Configuration clientConfiguration = new Configuration();
		clientConfiguration.setString("jobmanager.rpc.address", args[3]);
		clientConfiguration.setString("jobmanager.rpc.port", args[4]);
	//:	jobGraph.getJobConfiguration().setBoolean("job.keep_information", true);
//		if(args.length > 5){
//			jobGraph.setUpdated(new JobID(StringUtils.hexStringToByte(args[5])));
//		}
		System.out.println("Set Configuration");
		int retries = 5;
		while(retries > 0){
		try {
			JobClient jobClient = new JobClient(jobGraph, clientConfiguration);
			System.out.println("Created jobClient");
			
			try {
				jobClient.submitJobAndWait();
				retries= 0;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				retries = 0;
			}
			
		} catch (IOException e) {
			retries--;
			Thread.sleep(2000);
			//e.printStackTrace();
			
		}
		
		}
		System.out.println("Retried " + retries + " times. Stopping");
	}
}
