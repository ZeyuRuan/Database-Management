package bdsim.client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;
import bdsim.common.messages.BDSqlRequest;

/**
 * Simply connects to the server, takes SQL requests, and delegates the
 * response handling to the caller. 
 * 
 * @author acath, wpijewsk
 */
public class BDSimpleClient {

	private ObjectOutputStream m_requestWriter;
	private ObjectInputStream m_responseReader;
	private Socket m_socket;
	
	public BDSimpleClient(String host, int port) {
		try {
			m_socket = new Socket(host, port);
			m_requestWriter = new ObjectOutputStream(m_socket.getOutputStream());
			m_responseReader = new ObjectInputStream(m_socket.getInputStream());
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void request(String query, BDResponseHandler handler)
			throws IOException, ClassNotFoundException {
		m_requestWriter.writeObject(new BDSqlRequest(query));
		BDResponse response = (BDResponse) m_responseReader.readObject();
		if (handler != null && response != null) {
			response.handle(handler, 0);
		}
	// Commented out to allow more than one test to execute in the same session	
//		m_requestWriter.close();
//		m_responseReader.close();
//		m_socket.close();
	}
}
