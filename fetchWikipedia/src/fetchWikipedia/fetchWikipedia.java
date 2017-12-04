package fetchWikipedia;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;


public class fetchWikipedia {
	public static void main(String[] args){
		for(int i = 0; i < 100000; i++){
		try {
			//URL url = new URL("https://en.wikipedia.org/w/api.php?action=query&generator=random&prop=extracts&callback=onWikipedia&grnnamespace=0&format=json");
			String jsonText = readJsonFromUrl("https://en.wikipedia.org/w/api.php?action=query&generator=random&"
					+ "prop=extracts&callback=onWikipedia&grnnamespace=0&format=json&utf8=");
			
			 int titlestart = jsonText.indexOf("\"title\":");
		      String title = jsonText.substring(titlestart +9, jsonText.indexOf("\",", titlestart));
		      
		     int start = jsonText.indexOf("\"extract\":");
		      int end = jsonText.indexOf(">\"");
		      String extract = jsonText.substring(start+11,end+1);
		      extract = extract.replace("\\n" ,System.getProperty("line.separator"));
		      //System.out.println(extract);
		      String html = "<html><head><title> " + title + "</title></head><body>" + extract + "</body></html>";
		      String filename = "/home/marrus/Wikidata/" + title.replace(" " , "_") + ".html";
		      System.out.println(filename);
		      File htmlfile = new File(filename);
		     FileUtils.write(htmlfile, html);
			
			//System.out.println(jsonString);
//			JSONParser parser = new JSONParser();
//			JSONObject obj =  (JSONObject) parser.parse(jsonString);
//		      
//			JSONObject query =  (JSONObject) obj.get("query");
//			
//			JSONArray pages =  (JSONArray) query.get("pages");
//			
//			JSONObject content = (JSONObject) pages.get(0);
		         //System.out.println(content);
		        //System.out.println(content);
		
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  catch (ArrayIndexOutOfBoundsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		}
	}  
	
	 private static String readAll(Reader rd) throws IOException {
		    StringBuilder sb = new StringBuilder();
		    int cp;
		    while ((cp = rd.read()) != -1) {
		      sb.append((char) cp);
		    }
		    return sb.toString();
		  }

		  public static String readJsonFromUrl(String url) throws IOException {
		    InputStream is = new URL(url).openStream();
		    try {	
		      BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
		      String jsonText = readAll(rd);
//		      jsonText = jsonText.replace("/**/onWikipedia(", "");
//		      jsonText = jsonText.substring(0, jsonText.length()-1);
		    
		      
		      return jsonText;
		      
		    } finally {
		      is.close();
		    }
		  }
}
