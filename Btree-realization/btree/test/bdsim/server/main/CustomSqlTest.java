package bdsim.server.main;

import java.io.IOException;

import junit.framework.TestCase;
import bdsim.client.BDSimpleClient;
import bdsim.common.BDResponseAdapter;
import bdsim.common.messages.BDResultResponse;

/**
 * You can use this class as a starting point for JUnit tests. It connects
 * to the server on localhost (the server must already be running), then
 * executes the test* methods (like any JUnit test). You can then use 
 * m_connection.request() to easily execute SQL queries and handle the response.
 * See testPositiveNumberOfCustomers() for an example.
 * 
 * @author acath
 */
public class CustomSqlTest extends TestCase {
	
	/** Connection to the server */
	private BDSimpleClient m_connection;
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		m_connection = new BDSimpleClient("localhost", 1877);
	}

	/**
	 * m_connection.request() takes a query and a response handler. Response
	 * handlers implement the BDResponseHandler interface. For convenience,
	 * we've created a BDResponseAdapter which does nothing. The easiest way
	 * to handle the response is to use an anonymous subclass and override
	 * the handler method for the response type you're interested in.
	 * 
	 * @throws IOException If something goes wrong in the network layer.
	 * @throws ClassNotFoundException If the request type is not recognized.
	 * 		This really shouldn't happen.
	 */
	public void testPositiveNumberOfCustomers() throws IOException, ClassNotFoundException {
		m_connection.request("SELECT * FROM Customers", 
			new BDResponseAdapter() {
				@Override
				public void handle(BDResultResponse response, long elapsed) {
					assertTrue(response.getResults().getData().size() > 0);
				}
		});
	}
	
	
}
