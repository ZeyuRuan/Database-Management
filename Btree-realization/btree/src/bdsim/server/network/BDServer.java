package bdsim.server.network;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import bdsim.common.BDRequest;
import bdsim.common.BDRequestHandler;
import bdsim.common.BDResponse;
import bdsim.common.BDResultSet;
import bdsim.common.messages.BDCreateRequest;
import bdsim.common.messages.BDCreateResponse;
import bdsim.common.messages.BDDropRequest;
import bdsim.common.messages.BDDropResponse;
import bdsim.common.messages.BDErrorResponse;
import bdsim.common.messages.BDInfoRequest;
import bdsim.common.messages.BDInfoResponse;
import bdsim.common.messages.BDLoadRequest;
import bdsim.common.messages.BDLoadResponse;
import bdsim.common.messages.BDResultResponse;
import bdsim.common.messages.BDSaveRequest;
import bdsim.common.messages.BDSaveResponse;
import bdsim.common.messages.BDSqlRequest;
import bdsim.server.exec.BDParseException;
import bdsim.server.exec.BDPlan;
import bdsim.server.exec.BDPlanner;
import bdsim.server.exec.BDSemanticVisitor;
import bdsim.server.exec.BDSqlParser;
import bdsim.server.exec.BDSemanticVisitor.BDNameMappings;
import bdsim.server.exec.BDSemanticVisitor.SemanticException;
import bdsim.server.exec.nodes.NodeStatement;
import bdsim.server.system.BDSchema;
import bdsim.server.system.BDSystem;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTableManager.XmlException;
import bdsim.server.system.concurrency.BDTransaction;

/**
 * Listens for connections on a specified port, and processes incoming requests.
 * Note: clients are responsible for closing their own connections when they are
 * done using them. If clients continually leave connections open, then the
 * server may not have enough resources to continually service new connections.
 * 
 * @author wpijewsk
 * @revision $Id: BDServer.java 293 2007-01-21 16:28:18 +0000 (Sun, 21 Jan 2007)
 *           wpijewsk $
 */
public final class BDServer {

	// TODO wpijewsk Support multiquery operations
	
	// FIXME wpijewsk I don't think threads are getting returned correctly to
	// the thread pool.

	private class Handler implements Runnable {
		private final Socket clientSocket;

		/**
		 * Class constructor.
		 * 
		 * @param socket
		 *            The socket on which this handler should read and write
		 */
		Handler(Socket socket) {
			this.clientSocket = socket;
		}

		/**
		 * Handles the request from the client, processes the SQL in that
		 * request, and returns a response to the client.
		 */
		public void run() {
			logger.debug("Received connection from address "
					+ clientSocket.getInetAddress());

			ObjectOutputStream out = null;
			ObjectInputStream in = null;

			try {
				in = new ObjectInputStream(clientSocket.getInputStream());
				out = new ObjectOutputStream(clientSocket.getOutputStream());

				boolean listening = true;
				
				// FIXME wpijewsk It seems like clients should be responsible
				// for closing their own connections.
				while (listening) {
					BDRequest request = (BDRequest) in.readObject();
					BDResponse response = request.handle(m_requestHandler);
					out.writeObject(response);
					m_currid++;

					logger.debug("Sent response to client at "
							+ clientSocket.getInetAddress());
				}

				in.close();
				out.close();
				clientSocket.close();
			} catch (EOFException e) {
				logger.debug("Client " + clientSocket.getInetAddress()
						+ " closed its connection");
			} catch (IOException e) {
				e.printStackTrace();
				logger.debug("IOException caught: " + e.getMessage());
				assert(false);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				System.exit(-1);
			} finally {
				try {
					out.close();
					in.close();
					clientSocket.close();
				} catch (Exception e) {
				}
			}
			logger.debug("Thread " + Thread.currentThread().getName()
					+ " is exiting its run().");
		}
	}

	private class ServerRequestHandler implements BDRequestHandler {

		public BDResponse handle(BDInfoRequest request) {
			Map<String, BDSchema> m_schemas = new HashMap<String, BDSchema>();
			for (String name : BDSystem.tableManager.getTables().keySet()) {
				m_schemas.put(name, BDSystem.tableManager.getTableByName(name)
						.getSchema());
			}

			return new BDInfoResponse(m_schemas, BDSystem.tableManager
					.getFileName());
		}

		/**
		 * Handles a <code>BDSqlRequest</code>. Dispatches to the parser and
		 * executor, constructs a result set, and returns that result set along
		 * with any error messages or codes. Will return an error response if
		 * something is wrong with the query.
		 * 
		 * @return The server's response to a particular request
		 */
		public BDResponse handle(BDSqlRequest request) {

			logger.debug("Received SQL request");
			logger.debug("\t" + request.getSql());

			BDResponse response = null;
			BDResultSet resultSet = null;
			int TID = -1;

			try {
				BDTransaction trans = new BDTransaction();

				// Parse SQL
				BDSqlParser parser = new BDSqlParser();
				List<NodeStatement> stmtList = parser.parse(request.getSql());
				BDSemanticVisitor smtcVisitor = new BDSemanticVisitor();
				for (NodeStatement stmt : stmtList) {
					stmt.visit(smtcVisitor);

					if (smtcVisitor.resolveNames()) {
						// Process parsed statement
						BDNameMappings mappings = smtcVisitor.getMappings();
						BDPlanner planner = new BDPlanner(stmt);
						BDPlan plan = planner.makePlan(mappings);
						trans.addPlan(plan);
					}
				}

				TID = BDSystem.scheduler.addTransaction(trans);
				trans.setId(TID);
			} catch (BDParseException e) {
				logger.error("BDParseException: " + e.getLocalizedMessage());
				response = new BDErrorResponse(e.getLocalizedMessage());
				return response;
			} catch (SemanticException e) {
				logger.error("SemanticException: " + e.getLocalizedMessage());
				response = new BDErrorResponse(e.getLocalizedMessage());
				return response;
			}

			if (TID != -1) {
				// Blocking call to wait for response
				try {
					logger.debug("Handing task " + TID + " off to scheduler.");
					while (!BDSystem.scheduler.isFinished(TID)) {
						Thread.sleep(500);
						// FIXME Change this so the process is woken up when
						// ready instead of continually polling
					}
					logger.debug("Task finished, getting result set.");
					resultSet = BDSystem.scheduler.getResult(TID);
				} catch (Exception e) {
					response = new BDErrorResponse(e.getLocalizedMessage());
					logger.error("System failure, could not get response for query.");
					e.printStackTrace();
					logger.error("Caught Exception: " + e.getMessage());
					logger.error("Returning BDErrorResponse");
					return response;
				}
			}

			response = new BDResultResponse(resultSet);
			return response;
		}

		public BDResponse handle(BDCreateRequest request) {
			if (BDSystem.tableManager.getTableByName(request.getTableName()) != null) {
				return new BDErrorResponse("Table " + request.getTableName()
						+ " already exists");
			} else {
				BDSchema schema = new BDSchema(request.getNames(), request
						.getTypes());
				BDTable table = new BDTable(schema, request.getPrimaryKey(), request.getTableName());
				BDSystem.tableManager
						.createTable(request.getTableName(), table);
				return new BDCreateResponse(request.getTableName());
			}
		}

		public BDResponse handle(BDDropRequest request) {
			if (BDSystem.tableManager.getTableByName(request.getTableName()) == null) {
				return new BDErrorResponse("Table " + request.getTableName()
						+ " does not exist");
			} else {
				BDSystem.tableManager.dropTable(request.getTableName());
				return new BDDropResponse(request.getTableName());
			}
		}

		public BDResponse handle(BDLoadRequest request) {
			BDSystem.tableManager.readFromXmlFile(request.getDocument());
			return new BDLoadResponse();
		}

		public BDResponse handle(BDSaveRequest request) {
			try {
				return new BDSaveResponse(BDSystem.tableManager
						.getCurrentDatabase());
			} catch (XmlException e) {
				e.printStackTrace();
				return new BDErrorResponse(e.getLocalizedMessage());
			}
		}
	}
	static Logger logger = Logger.getLogger(BDServer.class);
	private int m_currid;
	private final ExecutorService m_pool;

	private ServerRequestHandler m_requestHandler;

	private ServerSocket m_serversocket;

	public BDServer() {
		m_serversocket = null;
		m_requestHandler = new ServerRequestHandler();
		int numthreads = Integer.parseInt(System
				.getProperty("network.numthreads"));
		m_pool = Executors.newFixedThreadPool(numthreads);
		m_currid = 1;
		logger.debug("Started a thread pool with " + numthreads + " threads");
	}

	public void shutDown() {
		// FIXME I get a SocketException when server socket is closed
		try {
			if (m_serversocket != null) {
				m_serversocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void startDbServer() {
		try {
			m_serversocket = new ServerSocket(Integer.parseInt(System
					.getProperty("network.port")), Integer.parseInt(System
							.getProperty("network.backlog")));

			logger.info("Server listening on port "
					+ m_serversocket.getLocalPort());

			while (true) {
				m_pool.execute(new Handler(m_serversocket.accept()));
			}
		} catch (IOException e) {
			e.printStackTrace();
			m_pool.shutdownNow();
			try {
				m_serversocket.close();
			} catch (IOException e1) {
			}
			System.exit(-1);
		} catch (NumberFormatException e) {
			e.printStackTrace();
			m_pool.shutdownNow();
			try {
				m_serversocket.close();
			} catch (IOException e1) {
			}
			System.exit(-1);
		} finally {
			try {
				m_serversocket.close();
			} catch (Exception e) {
			}
			m_pool.shutdown();
		}
		// FIXME I need a better way of shutting the server down here.
	}
}
