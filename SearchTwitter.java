/*
 * Author: Mengyu Li
 * student Id: 734953
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import mpi.*;
import java.util.Hashtable;

public class SearchTwitter {

	public static void main(String[] args) throws Exception {
		
		long startTime = System.currentTimeMillis();

		MPI.Init(args);
	
		int me = MPI.COMM_WORLD.Rank();
		
		int size = MPI.COMM_WORLD.Size();
		
		Hashtable<String, Integer> hash1 = new Hashtable<String, Integer>();

		Hashtable<String, Integer> hash2 = new Hashtable<String, Integer>();

		int count = 0; // count the number of times a certain word occurs

//		String filepath = "/Users/sumomoiemochi/Dropbox/2016Sm1Sty/" + "CloudComputing/Assignment1/miniTwitter.csv";
		String filepath = "twitter.csv";

		int linesCounted = countLine(filepath);

		BufferedReader input2 = new BufferedReader(new FileReader(filepath));

		int jobStart = linesCounted / size * me;

		int jobAssigned = linesCounted / size;

		System.out.print("Id:" + me + " read from " + jobStart + " lines,will continue for the next"+jobAssigned+" lines\n");
		
		count = searchStart(input2, jobStart, jobAssigned, hash1, hash2,linesCounted,args[3]);
			
		/*
		 * 
		 */

		if (me != 0) {
						
			processorContent processorS = new processorContent(count,hash1,hash2);
		     			
			byte[] buffer = processorContent.proToByte(processorS);

			MPI.COMM_WORLD.Send(buffer, 0, buffer.length, MPI.BYTE, 0,100);
						
		} else {
			processorContent processor = new processorContent(count,hash1,hash2);
						
			processorContent[] processorR = new processorContent[size-1];

			byte[] buffer = new byte[2000000];
			
			for (int i = 0; i < size-1; i++) {

				MPI.COMM_WORLD.Recv(buffer, 0, buffer.length, MPI.BYTE, i+1, 100);
				processorR[i] = processorContent.byteToPro(buffer);
				
				Merge2(processor,processorR[i]);				

			}
			
			List<Map.Entry<String, Integer>> listOfAnswers1 = new ArrayList<Map.Entry<String, Integer>>();
			listOfAnswers1 = sortByValue(processor.getHash1());
			
			List<Map.Entry<String, Integer>> listOfAnswers2 = new ArrayList<Map.Entry<String, Integer>>();
			listOfAnswers2 = sortByValue(processor.getHash2());

			
			System.out.print(" \nThe word '"+args[3]+"' has occured "+processor.getCount()+" times\n \n");
			
			System.out.print("The Top 10 Tweeters:\n");
			
			for(int i=0;i<10;i++){
				System.out.print(listOfAnswers1.get(i).getKey()+" ( "+listOfAnswers1.get(i).getValue()+" ) \n");
			}
			
			System.out.print("\n \n \nThe Top 10 Topics:\n");
			
			for(int i=0;i<10;i++){
				System.out.print(listOfAnswers2.get(i).getKey()+" ( "+listOfAnswers2.get(i).getValue()+" ) \n");
			}
			System.out.print("\n \n");
		
			
		}
		
		

		 MPI.Finalize();
		 
		 if(me==0){
		 long endTime   = System.currentTimeMillis();
		 long totalTime = endTime - startTime;
		 System.out.println("The program running time: "+totalTime+"\n");
		 }

	}


	public static int SearchWords(String cell2, String p2) {
		int count = 0;
		Pattern pattern = Pattern.compile("(([[^a-zA-Z]+|\\s](" + p2 + "))|^" + p2 + ")[[^a-zA-Z]*|\\s|\\$]");
		Matcher matcher = pattern.matcher(cell2);
		while (matcher.find()) {
			count++;
		}
		return count;
	}

	

	/*
	 * 
	 */

	public static String SearchTopic(String cell2) {
		Pattern pattern = Pattern.compile("#([a-zA-Z0-9_])");
		Matcher matcher = pattern.matcher(cell2);
		while (matcher.find()) {
			return matcher.group();
		}
		return null;
	}

	public static void putInHash(Hashtable<String, Integer> hash, String cell) {
		if (hash.containsKey(cell)) {
			int valueOfKey = hash.get(cell);
			hash.put(cell, valueOfKey + 1);
		} else {
			hash.put(cell, 1);
		}
	}

	public static void showHash(Hashtable<String, Integer> hash) {

		for (String key : hash.keySet()) {
			int value = hash.get(key);
			System.out.println(key + " " + value);
		}
		
	}

	public static List<Map.Entry<String, Integer>> sortByValue(Hashtable<String, Integer> table) {

		// sort the hashtable by value
		List<Map.Entry<String, Integer>> listOfAnswers = new ArrayList<Map.Entry<String, Integer>>(table.entrySet());

		// Our comparator defines how to sort our Key/Value pairs. We sort
		// by the highest value.
		java.util.Collections.sort(listOfAnswers, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();
			}
		});

		return listOfAnswers;
	}

	/*
	 * method to count the line
	 */

	public static int countLine(String filepath) throws IOException{
		
	BufferedReader input = new BufferedReader(new FileReader(filepath));
	
	int linesCounted = 0;

	while (input.readLine() != null) {

		linesCounted++;

	}
	input.close();
	return linesCounted;
	}

	public static int searchStart(BufferedReader input2,int jobStart,int jobAssigned,
			Hashtable<String,Integer> hash1,Hashtable<String,Integer> hash2,int linesCounted,String keyWord) throws IOException{
		
	String cell;
	int count = 0;
	
	for (int i = 0; i < linesCounted; i++) {
		
		// the content of twitter is stored in cell

		if (i >= jobStart && i < jobStart + jobAssigned) {
			
			cell = input2.readLine().toLowerCase();

			/*
			 * NO1
			 */
			count += SearchWords(cell, keyWord.toLowerCase());
			
			 /*
			 * NO2
			 */

			Pattern pattern = Pattern.compile("@([a-zA-Z0-9_]{1,15})");
			Matcher matcher = pattern.matcher(cell);
			while (matcher.find()) {
				putInHash(hash1,matcher.group());
			}
			

			/*
			 * NO3
			 */
				Pattern pattern2 = Pattern.compile("#([a-zA-Z0-9_]+)");
				Matcher matcher2 = pattern2.matcher(cell);
				while (matcher2.find()) {
					putInHash(hash2,matcher2.group());
				}

	
		} else{
		input2.readLine();
		}
	}
	
	 return count;

	}
	
	
	public static void putHashTogether(Hashtable<String, Integer> hashA,Hashtable<String, Integer> hashB){
		for (String key : hashB.keySet()) {
			int value = hashB.get(key);
			hashA.put(key, value);
		}
		
	}
	
	
	
	
	public static void Merge2(processorContent processor,processorContent processorS){
		
		processor.setCount(processor.getCount()+processorS.getCount()) ;
						
		for(String key: processorS.getHash1().keySet()) {
			if (processor.getHash1().containsKey(key)) {
				int valueOfKey = processor.getHash1().get(key);
				int valueOfSKey = processorS.getHash1().get(key);
				processor.getHash1().put(key, valueOfKey + valueOfSKey);
			} else {
				int valueOfSKey = processorS.getHash1().get(key);
				processor.getHash1().put(key,valueOfSKey);			}
		}
		
		
		for(String key: processorS.getHash2().keySet()) {
			if (processor.getHash2().containsKey(key)) {
				int valueOfKey = processor.getHash2().get(key);
				int valueOfSKey = processorS.getHash2().get(key);
				processor.getHash2().put(key, valueOfKey + valueOfSKey);
			} 
		}
		
	}
	
}
