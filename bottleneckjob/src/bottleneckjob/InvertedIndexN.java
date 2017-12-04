package bottleneckjob;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;


public class InvertedIndexN {

	//private LinearHashMap<String> hashmap;
	private Map<String, IndexRecordN> hashmap = new HashMap<String, IndexRecordN>();
	private Set<String> stopwords;
	private int noOfDistinctKeywords;

	private static Set<Character> charactersToStrip;
	static {
		charactersToStrip = new HashSet<Character>();
		charactersToStrip.addAll(Arrays.asList(',', ';', ',', '.', ':', '-',
				'_', '#', '\'', '+', '*', '~', '`', '´', '?', '\\', '=', '}',
				')', ']', '(', '[', '(', '/', '{', '&', '%', '$', '§', '"',
				'!', '^', '<', '>', '|'));
	}

	public InvertedIndexN() throws IOException {
		//this.hashmap = new LinearHashMap<String>();
		this.noOfDistinctKeywords = 0;
		loadStopWords();
	}

	private void loadStopWords() throws IOException {
		stopwords = new HashSet<String>();
		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream("/home/marrus/stopwords.txt")));

		String line;
		while ((line = in.readLine()) != null) {
			String word = preprocessKeyword(line);
			if (keywordIsValid(word)) {
				stopwords.add(word);
			}
		}
		in.close();
	}

	public void put(String keyword, String document) {
		keyword = preprocessKeyword(keyword);

		if (keywordIsValid(keyword) && !stopwords.contains(keyword)) {
			insertIntoInvertedIndex(keyword, document);
		}
	}
	
	public Iterator<String> getAllKeys() {
		
		return this.hashmap.keySet().iterator();
	}
	
	public IndexRecordN getByKey(String key) {
		return this.hashmap.get(key);
	}

	private void insertIntoInvertedIndex(String keyword, String document) {
		
		IndexRecordN record = hashmap.get(keyword);
		if (record == null) {
			record = new IndexRecordN();
			record.key = keyword;
			hashmap.put(keyword, record);
			//hashmap.insert(record);
			noOfDistinctKeywords++;
		}

		if (!record.documents.contains(document)) {
			record.documents.add(document);
		}
	}

	private boolean keywordIsValid(String keyword) {
		return keyword.length() >= 3;
	}

	private String preprocessKeyword(String keyword) {
		keyword = keyword.trim().toLowerCase();

		StringBuilder strippedKeyword = new StringBuilder();
		for (char c : keyword.toCharArray()) {
			if (!charactersToStrip.contains(c)) {
				strippedKeyword.append(c);
			}
		}
		return keyword;
	}

	public int getNoOfDistinctKeywords() {
		return noOfDistinctKeywords;
	}
}
