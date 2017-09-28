/*
 * Author: Mengyu Li
 * student Id: 734953
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Hashtable;

public class processorContent implements java.io.Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	int count;
	
	private Hashtable<String,Integer> hash1;
	
	private Hashtable<String,Integer> hash2;
	
	public processorContent(){
		this.count = 0;
		this.hash1 = null;
		this.hash2 = null;
	}
	
	public void setCount(int count2){
		this.count = count2;
	}
	
	
	
	public processorContent(int count,Hashtable<String,Integer> hash1,
									Hashtable<String,Integer> hash2){
		
		this.count = count;
		this.hash1 = hash1;
		this.hash2 = hash2;
		
	}
	
	
	public Hashtable<String,Integer> getHash1(){
		return this.hash1;
	}
	
	public Hashtable<String,Integer> getHash2(){
		return this.hash2;
	}
	
	public int getCount(){
		return this.count;
		
	}	
	

public static processorContent byteToPro(byte[] buffer) throws IOException, ClassNotFoundException{
	ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
	processorContent processorR=new processorContent();
	
	ObjectInput in = null;
	try {
		in = new ObjectInputStream(bis);
		processorR = (processorContent)in.readObject();

	} finally {
		try {
			bis.close();
		} catch (IOException ex) {
			// ignore close exception
		}
		try {
			if (in != null) {
				in.close();
			}
		} catch (IOException ex) {
			// ignore close exception
		}

	}
	return processorR;

}


public static byte[] proToByte(processorContent processorS) throws IOException {
	ByteArrayOutputStream bos = new ByteArrayOutputStream();
	ObjectOutput out = null;
	byte[] myBytes;
	try {
		out = new ObjectOutputStream(bos);
		out.writeObject(processorS);
		myBytes = bos.toByteArray();
	} finally {
		try {

			if (out != null) {
				out.close();
			}
		} catch (IOException ex) {
			// ignore close exception
		}
		try {
			bos.close();
		} catch (IOException ex) {
			// ignore close exception
		}
	}
	return myBytes;
	}

}



