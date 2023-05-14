package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;
import java.util.Map;

/* any necessary Java packages here */

public class RUStoreServer {

	/* any necessary class members here */
	
	private static ServerSocket server_soc;
	private static Socket client_soc;
	private static DataOutputStream out ;
	private static BufferedReader in ;
    private static Hashtable<String, byte[]> storage = new Hashtable<String, byte[]>();
    private static ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
	/* any necessary helper methods here */

	/**
	 * RUObjectServer Main(). Note: Accepts one argument -> port number
	 * @throws IOException 
	 */
    public static void get_the_file() throws IOException {
    	out.writeBytes("Send Key\n");
		String key = in.readLine() ;
	
	
		if (storage.containsKey(key)) {

			out.writeBytes("Key is here\n") ;
			

			byte[] output = storage.get(key) ;


			int length = output.length;

			out.writeBytes(length + "\n") ;

			

			 ByteArrayInputStream bais = new ByteArrayInputStream(output);
		        BufferedInputStream bis = new BufferedInputStream(bais);

		        byte[] buffer = new byte[length];
		        int bytesRead = bis.read(buffer, 0, length);

		        if (bytesRead != length) {
		            throw new IOException("Failed to read entire file");
		        }

		        out.write(buffer);
		        out.flush();

		
			
		} else {

			out.writeBytes("Key is not here\n") ;


		}
    }
    public static void remove() throws IOException {
    	out.writeBytes("Send the key\n");
		String key = in.readLine() ;

		if (storage.containsKey(key)) {

			out.writeBytes("Key exists and will be removed\n");
			storage.remove(key) ;

		} else {

			out.writeBytes("ABSENT\n");

		}
    }
    public static void put_the_data() throws IOException {

		out.writeBytes("Send Key\n");
		String key = in.readLine() ;
		

		if (storage.containsKey(key)) {

			out.writeBytes("Key Exists\n") ;

		} else {

			out.writeBytes("include\n") ;
		
				byte[] input = in.readLine().getBytes() ;

				storage.put(key, input) ;
		}
    }
    
    public static void put_the_file() throws IOException {
    	out.writeBytes("Send Key\n");
		String key = in.readLine() ;
		if(storage.contains(key)==false) {

			out.writeBytes("Start input\n") ;

			int length = Integer.parseInt(in.readLine());
		

			int bytesRead = 0 ;

			byte[] buffer = new byte[length] ;

			

			while ((bytesRead < length)) {
				
				int count = client_soc.getInputStream().read(buffer, bytesRead, (length - bytesRead)) ;
				
				bytesRead += count ;
			

			}
			
			baos.write(buffer) ;

			
			byte[] fullBytes = baos.toByteArray() ;
			storage.put(key, fullBytes) ;

			
			

				
	
		}
		else if(storage.containsKey(key)) {
			out.writeBytes("The Key Exists") ;
		}
		
		

    }
    public static void  getTheData() throws IOException {
    	out.writeBytes("Send Key\n");
		String key = in.readLine() ;
		

		if (storage.containsKey(key)) {

			out.writeBytes("The Key exists here\n") ;
		

			byte[] output = storage.get(key) ;

			String out_string = new String(output);

			out.writeBytes(out_string + "\n");
			

		} else {

			out.writeBytes("Key is not Here\n") ;
		

		}
    }
    
    
    public static void listTheObjects() throws IOException {
    	out.writeBytes(storage.size()+ "\n");
    	if(storage.size()==0) {
    	    System.out.println("List is Empty");
    		}
    	if(storage.size()!=0) {
    		for(String val: storage.keySet()) {
				 out.writeBytes(val + "\n");
			}
    	}
		
    }
	public static void main(String args[]) throws IOException{

		// Check if at least one argument that is potentially a port number
		if(args.length != 1) {
			System.out.println("Invalid number of arguments. You must provide a port number.");
			return;
		}

		// Try and parse port # from argument
		int port = Integer.parseInt(args[0]);
	
		server_soc = new ServerSocket(port);
		
		System.out.println("Server is running....");
		while (true) {
			System.out.println("Connection is active..");
			client_soc = server_soc.accept(); // server accepts client
			System.out.println("Client is connected....");
			out = new DataOutputStream(client_soc.getOutputStream()) ;
			in = new BufferedReader(new InputStreamReader(client_soc.getInputStream())) ;
			String input_message; // reads the message
	
			while(((input_message = in.readLine()) != null) && (!input_message.equals("DISCONNECT"))) {
				//input_message =in.readLine();
			switch(input_message){
				case "Insert the data": {	
					put_the_data();
					break ;
					
				}
				case "Get Data from the Server": {
					getTheData();
					break ;

				}
				case("List Objects"):{
				
					listTheObjects();
					break;
				}
				case "Insert the file": {
					
					put_the_file();
					break ;

				}
				case "Get The File": {
					
					get_the_file();

					break ;

				}
				case "Remove the Key": {
					
					remove();

					break ;

				}
				}

			
			}
		
			out.close();
			in.close();
			client_soc.close();

		}

	}
}

//tried to use if else statements but did not work
//code dumps 

//if(input_message.equals("insert data"):{
//out.writeBytes("Send Key\n");
//String key = in.readLine();
//if(storage.contains(key)) { 
//	out.writeBytes("Already exists\n");
//}else {
//	out.writeBytes("Provide the data\n");
//	byte[] bytes = in.readLine().getBytes();
//	Byte[] second = new Byte[bytes.length];
//	for(int i = 0; i < second.length;i++) {
//		second[i]=bytes[i];
//	}
//	storage.put(key, second);
//  
//}
//break;
//}
//if(input_message.equals("insert data")) {
////System.out.println("Reaches here 3");
//out.writeBytes("Send Key\n");
////System.out.println("Reaches here 4");
//String key = in.readLine();
//if(storage.containsKey(key)){
//	//System.out.println("Reaches here 5");
//	out.writeBytes("Already exists\n");
//}else {
//	out.writeBytes("Provide the data\n");
//	//System.out.println("reaches here 6");
//	byte[] input = in.readLine().getBytes();
//	storage.put(key, input);
//	//System.out.println(storage);
//	
//}
//break;
//
//}
//else if(input_message.equals("List Objects")) {
//System.out.println("Entered List");
//out.writeBytes(storage.size()+ "\n");
//if(storage.size()==0) {
////	System.out.println("List is Empty");
//	
//}else {
//	for(String key: storage.keySet()) {
//		System.out.println("reaches in the for loop");
//		out.writeBytes(key + "\n");
//	}
//	System.out.println("here");
//	
//}
//
//}