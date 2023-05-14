package com.RUStore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/* any necessary Java packages here */

public class RUStoreClient {

	/* any necessary class members here */
private Socket client_soc;
private String host;
private int port;
private BufferedReader in;
private DataOutputStream out;
//private ByteArrayOutputStream baos = new ByteArrayOutputStream() ;

	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {
		this.host = host;
		this.port=port;
		// Implement here

	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void connect() {
	
			try {
				client_soc = new Socket(host,port);
				
				
				out = new DataOutputStream(client_soc.getOutputStream());
				in = new BufferedReader(new InputStreamReader(client_soc.getInputStream()));
			} catch (IOException e) {
				
				e.printStackTrace();
			}
			
	
		
		// Implement here

	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, byte[] data) throws IOException {
		// Implement here
				//System.out.println("writing put data");
				out.writeBytes("Insert the data\n") ;

				//System.out.println("wrote, waiting...");

				String msg = in.readLine() ;
		

				if (msg.equals("Send Key")) {
					//now we are sending the the server
					out.writeBytes(key + "\n") ;
					//read the new message from the server 
					msg = in.readLine() ;
				}
					if (msg.equals("Key Exists")) {

						
						return 1 ;

					} 
					
					else if (msg.equals("include")) {
						
						String input = new String(data);

						out.writeBytes(input + "\n") ;

					} 

				
//		out.writeBytes("insert data\n");
//		String msg = in.readLine();
//		if(msg.equals("Send key")) {
//			out.writeBytes(key + "\n");
//			msg=in.readLine();
//		}
//		if(msg.equals("Already exists")) {
//			return 1;
//		}
//		if(msg.equals("Provide the data")) {
//			out.writeBytes(new String(data) + "\n");
//		}
//		
//
//		return 0;

	
				return 0 ;
	}

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int put(String key, String file_path) throws IOException {
	    out.writeBytes("Insert the file\n");

	    String msg = in.readLine();

	    if (msg.equals("Send Key")) {
	        out.writeBytes(key + "\n");

	        msg = in.readLine();

	        if (msg.equals("The Key Exists")) {
	            return 1;
	        } else if (msg.equals("Start input")) {
	            Path path = Paths.get(file_path);
	            long file_len = Files.size(path);
	            out.writeBytes(file_len + "\n");

	            byte[] data = Files.readAllBytes(path);
	            out.write(data);
	            out.flush();
	        }
	    }

	    return 0;
	}


	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		object data as a byte array, null if key doesn't exist.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public byte[] get(String key) throws IOException {

		out.writeBytes("Get Data from the Server\n") ;



		String msg = in.readLine() ;
		
		if (msg.equals("Send Key")) {

			out.writeBytes(key + "\n") ;

			msg = in.readLine() ;

			if (msg.equals("The Key exists here")) {

			// You recieve back bytes of the data from the server 
				byte[] data = in.readLine().getBytes();
				return data ;
				

			} else if (msg.equals("Key is not Here")) {

		
			System.out.println("THEY KEY DOES NOT EXIST!!!!!!!");

			}

		}

		return null;

	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key	key associated with the object
	 * @param	file_path	output file path
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int get(String key, String file_path) throws IOException {

		// Implement here

		
				out.writeBytes("Get The File\n") ;


				String msg = in.readLine();
				

				if (msg.equals("Send Key")) {

					out.writeBytes(key + "\n") ;

					msg = in.readLine() ;

					if (msg.equals("Key is here")) {

					
						int length = Integer.parseInt(in.readLine()) ;

						FileOutputStream fileOut = new FileOutputStream(file_path) ;
						BufferedOutputStream bufferedOut = new BufferedOutputStream(fileOut) ;

						 byte[] buffer = new byte[length];
					        int bytesRead = 0;

					        while (bytesRead < length) {
					            int count = client_soc.getInputStream().read(buffer, bytesRead, (length - bytesRead));
					            if (count == -1) {
					                throw new IOException("Failed to read entire file");
					            }
					            bytesRead += count;
					        }

					        bufferedOut.write(buffer, 0, length);
					        bufferedOut.flush();
					        bufferedOut.close();

					} else if (msg.equals("Key is not here")) {

						return 1 ;

					}

				}

				return 0;

	}

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 * @throws IOException 
	 */
	public int remove(String key) throws IOException {

		out.writeBytes("Remove the Key\n") ;

		String msg = in.readLine() ;

		if (msg.equals("Send the key")) {

			out.writeBytes(key + "\n") ;

			msg = in.readLine() ;
		}
			if (msg.equals("Key exists and will be removed")) {
				 System.out.println("Server is removing the key....");
				return 0;

			} else if (msg.equals("ABSENT")) {

			 System.out.println("Key is not here");

			}

		


		return 1;


	}

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 * @throws IOException 
	 */
	public String[] list() throws IOException {
	    out.writeBytes("List Objects\n");
	    int storage_size = Integer.parseInt(in.readLine());
	    if (storage_size == 0) {
	        return null;
	    }
	    //reads the storage size and stores is in a list 
	    //limit only takes first storage size 
	    String[] list = in.lines().limit(storage_size).toArray(String[]::new);
	    return list;
	}


	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 * @throws IOException 
	 */
	public void disconnect() throws IOException {
        out.close();
        in.close();
        client_soc.close();
		// Implement here

	}

}
