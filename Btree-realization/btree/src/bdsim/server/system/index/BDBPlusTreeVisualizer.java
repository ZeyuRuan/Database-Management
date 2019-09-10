package bdsim.server.system.index;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.Timer;

import bdsim.client.BDSimpleClient;
import bdsim.common.BDResponseHandler;
import bdsim.common.messages.BDCreateResponse;
import bdsim.common.messages.BDDropResponse;
import bdsim.common.messages.BDErrorResponse;
import bdsim.common.messages.BDInfoResponse;
import bdsim.common.messages.BDLoadResponse;
import bdsim.common.messages.BDResultResponse;
import bdsim.common.messages.BDSaveResponse;
import bdsim.server.system.BDTable;
import bdsim.util.StringUtil;

public class BDBPlusTreeVisualizer extends javax.swing.JFrame implements
		ActionListener {
		
	public class BDBPlusTestScriptRunner {

		private class CheckExistsResponseHandler extends ResponseAdapter {
			@Override
			public void handle(BDResultResponse response, long elapsed) {
				int rowsReturned = response.getResults().getData().size(); 
				switch (rowsReturned) {
				case 0:
					printError("Row not found (" + m_line + ")");
					break;
				case 1:
					printInfo("Success (" + m_line + ")");
					break;
				default:
					printError("Key is not unique, " + rowsReturned + " rows were found (" + m_line + ")");
					break;					
				}
			}
		}
		
		private class CheckNotExistsResponseHandler extends ResponseAdapter {
			@Override
			public void handle(BDResultResponse response, long elapsed) {
				int rowsReturned = response.getResults().getData().size(); 
				switch (rowsReturned) {
				case 0:
					printInfo("Success (" + m_line + ")");
					break;
				case 1:
					printError("Row should not exist (" + m_line + ")");
					break;
				default:
					printError("Key is not unique, " + rowsReturned + " rows were found (" + m_line + ")");
					break;					
				}
			}
		}
		
		private class DeleteResponseHandler extends ResponseAdapter {
			
		}
		
		private class InsertResponseHandler extends ResponseAdapter {
			
		}
		
		private class ResponseAdapter implements BDResponseHandler {

			protected int m_line;
			
			public void handle(BDCreateResponse response, long elapsed) {
				assert false;
			}
			
			public void handle(BDDropResponse response, long elapsed) {
				assert false;
			}

			public void handle(BDErrorResponse response, long elapsed) {
				printError(response.getMessage());
			}

			public void handle(BDInfoResponse response, long elapsed) {
				assert false;
			}

			public void handle(BDLoadResponse response, long elapsed) {
				assert false;				
			}

			public void handle(BDResultResponse response, long elapsed) {
				
			}

			public void handle(BDSaveResponse response, long elapsed) {
				assert false;
			}

			public void setLine(int line) {
				m_line = line;
			}
			
		}

		private CheckExistsResponseHandler m_checkExistsResponseHandler;
		
		private CheckNotExistsResponseHandler m_checkNotExistsResponseHandler;
		
		private DeleteResponseHandler m_deleteResponseHandler;
		
		private InsertResponseHandler m_insertResponseHandler;
		
		private LineNumberReader m_script;
				
		private StringBuilder m_stdout;
		
		private StringBuilder m_stderr;
		
		public BDBPlusTestScriptRunner(LineNumberReader script) {
			m_script = script;
			m_insertResponseHandler = new InsertResponseHandler();
			m_deleteResponseHandler = new DeleteResponseHandler();
			m_checkExistsResponseHandler = new CheckExistsResponseHandler();
			m_checkNotExistsResponseHandler = new CheckNotExistsResponseHandler();
			m_stdout = new StringBuilder();
			m_stderr = new StringBuilder();
		}
		
		public void printInfo(String msg) {
			m_stdout.append(msg);
			m_stdout.append("\n");
		}
		
		public void printError(String msg) {
			m_stderr.append(msg);
			m_stderr.append("\n");
		}
		
		public void run() throws IOException {
			String line;
			while ((line = m_script.readLine()) != null && line.length() > 0) {
				String[] parts = line.split(" ");
				if (parts.length != 2) {
					printError("Corrupt line (" + m_script.getLineNumber() + ")");
				} else {
					String opcode = parts[0];
					String arg = parts[1];
					doOperation(opcode, arg, m_script.getLineNumber());
				}
			}
		}
		
		public String getInfo() {
			return m_stdout.toString();
		}
		
		public String getErrors() {
			return m_stderr.toString();
		}
		
		private void doOperation(String opcode, String arg, int line) throws IOException {
			try {
				if ("insert".equals(opcode)) {
					m_insertResponseHandler.setLine(line);
					makeInsertQuery(arg, m_insertResponseHandler);
				} else if ("delete".equals(opcode)) {
					m_deleteResponseHandler.setLine(line);
					makeDeleteQuery(arg, m_deleteResponseHandler);
				} else if ("checkExists".equals(opcode)) {
					m_checkExistsResponseHandler.setLine(line);
					makeCheckExistsQuery(arg, m_checkExistsResponseHandler);
				} else if ("checkNotExists".equals(opcode)) {
					m_checkNotExistsResponseHandler.setLine(line);
					makeCheckNotExistsQuery(arg,
							m_checkNotExistsResponseHandler);
				} else {
					printError("Illegal opcode (" + line + "): " + opcode);
					return;
				}
			} catch (ClassNotFoundException e) {
				printError("Response type not recognized (" + line + "): " + e.getMessage());
			}
		}
		
	}

	private static final long serialVersionUID = 1L;

	private BDBPlusTreeDrawer m_drawer;

	private JTextField m_numberField;

	private BDTable m_table;

	private String m_tableName;

	private BDBPlusTreeIndex m_tree;

	private BDSimpleClient m_connection;
	
	public BDBPlusTreeVisualizer() {
		// Do nothing...
	}
	
	public BDBPlusTreeVisualizer(String tableName, BDTable table) {
		super("BDBPlusTreeVisualizer");
				
		m_tableName = tableName;
		m_table = table;
		m_tree = (BDBPlusTreeIndex)table.getPrimaryIndex();
		m_tree.setVisualizer(this);
		
		Timer t = new Timer(3000, new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				m_connection = new BDSimpleClient("localhost", 
						Integer.parseInt(System.getProperty("network.port")));
			}
		});
		t.setRepeats(false);
		t.start();
		
		initComponents();
	}

	public void actionPerformed(ActionEvent e) {
		
		if ((e.getActionCommand()).equals("Insert")) {
			
			try {
				makeInsertQuery(Integer.parseInt(m_numberField.getText()) + "", null);
			} catch (NumberFormatException e3) {
				popError("You can only insert integer values.");
			} catch (IOException e1) {
				popError(e1.getMessage());
				e1.printStackTrace();
			} catch (ClassNotFoundException e2) {
				assert false;
			}
			
		} else if ((e.getActionCommand()).equals("Delete")) {
						
			try {
				makeDeleteQuery(Integer.parseInt(m_numberField.getText()) + "", null);
			} catch (NumberFormatException e3) {
				popError("You can only delete integer values.");
			} catch (IOException e1) {
				popError(e1.getMessage());
				e1.printStackTrace();
			} catch (ClassNotFoundException e2) {
				assert false;
			}
		
		} else if ((e.getActionCommand()).equals("Load Script")) {
			
			JFileChooser chooser = new JFileChooser();
			if (chooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
				try {
					BDBPlusTestScriptRunner test = 
						new BDBPlusTestScriptRunner(
								new LineNumberReader(
										new FileReader(chooser.getSelectedFile())));
					test.run();
					JOptionPane.showMessageDialog(this,
							"Errors:\n" + test.getErrors() + "\nInfo:\n" + test.getInfo(),
							"Script results: " + chooser.getSelectedFile().toString(),
							JOptionPane.PLAIN_MESSAGE);
					
				} catch (FileNotFoundException e1) {
					popError("Test file not found: " + e1.getMessage());
				} catch (IOException e2) {
					popError("Failure reading test script: " + e2.getMessage());
				}
			}
			
		} else if ((e.getActionCommand()).equals("Close")) {
			
			dispose();
		
		}
		
	}
	
	public void loadTestScript(String file) {
		try {
			BDBPlusTestScriptRunner test = 
				new BDBPlusTestScriptRunner(
						new LineNumberReader(
								new FileReader(file)));
			test.run();
			System.out.println(
					"Errors:\n" + test.getErrors() + "\nInfo:\n" + test.getInfo() + "\n" +
					"Script results: " + file);			
		} catch (FileNotFoundException e1) {
			System.err.println("Test file not found: " + e1.getMessage());
		} catch (IOException e2) {
			System.err.println("Failure reading test script: " + e2.getMessage());
		}
		return;
	}

	public void treeUpdatedHandler() {
		m_drawer.repaint();
	}

	private void initComponents() {

		setTitle(m_tableName + " index");
		
		m_drawer = new BDBPlusTreeDrawer(m_tree);

		JScrollPane mainPane = new JScrollPane(m_drawer);

		JPanel buttonPanel = new JPanel();

		JLabel numberLabel = new JLabel("Number: ");
		buttonPanel.add(numberLabel);

		m_numberField = new JTextField(8);
		buttonPanel.add(m_numberField);

		JButton insertButton = new JButton("Insert");
		insertButton.setActionCommand("Insert");
		insertButton.addActionListener(this);
		buttonPanel.add(insertButton);

		JButton deleteButton = new JButton("Delete");
		deleteButton.setActionCommand("Delete");
		deleteButton.addActionListener(this);
		buttonPanel.add(deleteButton);
		
		JButton loadScriptButton = new JButton("Load Test Script");
		loadScriptButton.setActionCommand("Load Script");
		loadScriptButton.addActionListener(this);
		buttonPanel.add(loadScriptButton);
		
		JButton quitButton = new JButton("Close");
		quitButton.setActionCommand("Close");
		quitButton.addActionListener(this);
		buttonPanel.add(quitButton);

		buttonPanel.setSize(200, 200);

		getContentPane().setLayout(new BorderLayout());

		getContentPane().add(mainPane, BorderLayout.CENTER);
		getContentPane().add(buttonPanel, BorderLayout.SOUTH);

		pack();
	}

	private void makeCheckExistsQuery(String value, BDResponseHandler handler) throws IOException, ClassNotFoundException {
		String query = 
			"SELECT * FROM " + m_tableName + 
			" WHERE " + m_tree.getKeyName() + "=" + value;
		m_connection.request(query, handler);
	}

	private void makeCheckNotExistsQuery(String value, BDResponseHandler handler) throws IOException, ClassNotFoundException {
		makeCheckExistsQuery(value, handler);
	}

	private void makeDeleteQuery(String value, BDResponseHandler handler) throws IOException, ClassNotFoundException {
		String query = 
			"DELETE FROM " + m_tableName + 
			" WHERE " + m_tree.getKeyName() + "=" + value;
		m_connection.request(query, handler);
	}

	private void makeInsertQuery(String value, BDResponseHandler handler) throws IOException, ClassNotFoundException {
		Vector<String> values = new Vector<String>();
		for (int ii = 0; ii < m_table.getSchema().getNames().size(); ii++) {
			values.add("''");
		}
		values.set(m_table.getSchema().getPosition(m_tree.getKeyName()), value);
		String query =
			"INSERT INTO " + m_tableName + 
			" VALUES " +
			"(" + StringUtil.join(values) + ")";
		m_connection.request(query, handler);
	}

	private void popError(String message) {
		JOptionPane.showMessageDialog(this, message);
	}
}