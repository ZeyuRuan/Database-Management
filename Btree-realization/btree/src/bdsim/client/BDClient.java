package bdsim.client;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JTextPane;
import javax.swing.ListSelectionModel;
import javax.swing.UIManager;
import javax.swing.plaf.metal.MetalLookAndFeel;
import javax.swing.table.DefaultTableModel;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import bdsim.common.BDRequest;
import bdsim.common.BDResponse;
import bdsim.common.BDResponseHandler;
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
import bdsim.server.system.BDSchema;
import bdsim.server.system.BDTableManager.XmlException;

/**
 * Database client application. Used to connect to a host, submit queries, and
 * display the results.
 * 
 * @author wpijewsk
 * @revision $Id: BDClient.java 197 2006-05-15 00:56:37 +0000 (Mon, 15 May 2006) wpijewsk $
 */
public final class BDClient extends JFrame {

	// TODO Improve color scheme of GUI

	private class ClientResponseHandler implements BDResponseHandler {

		public void handle(BDCreateResponse response, long elapsed) {
			m_statuslabel.setText("Created table " + response.getTableName());
			makeRequest(new BDInfoRequest());
		}

		public void handle(BDDropResponse response, long elapsed) {
			m_statuslabel.setText("Dropped table " + response.getTableName());
			makeRequest(new BDInfoRequest());
		}

		public void handle(BDErrorResponse response, long elapsed) {
			m_statuslabel.setText(response.getMessage());
		}

		public void handle(BDInfoResponse response, long elapsed) {
			m_schemas = response.getSchemas();
			String msg = "";
			for (String name : response.getSchemas().keySet()) {
				BDSchema schema = response.getSchemas().get(name);
				
				msg += name + " (";
				for(String field : schema.getNames()) {
					msg += field + ", ";
				}
				msg = msg.substring(0, msg.length() - 2);
				msg += "), ";			
			}
			msg = msg.substring(0, msg.length() - 2);
			m_currentTables.setText(msg);
			m_currentDb.setText(response.getFileName());
		}

		public void handle(BDLoadResponse response, long elapsed) {
			m_statuslabel.setText("Loaded new database ["
					+ (((double) elapsed) / 1000.) + "s]");
			makeRequest(new BDInfoRequest());
		}

		public void handle(BDResultResponse response, long elapsed) {
			int numrecords = 0;
			if (response.getResults() != null) {
				numrecords = response.getResults().getData().size();
			}
			removeAllData();

			m_statuslabel.setText("Retrieved " + numrecords
					+ " records from server [" + (((double)elapsed)/1000.) + "s]");

            if (response.getResults() != null) {

				BDSchema schema = response.getResults().getSchema();

				if (schema == null || schema.getNames() == null) {
					logger.error("UGH: schema.getNames() returned null!");
				} else {

					if (schema.getNames().size() > 0) {
						for (String str : schema.getNames()) {
							m_tablemodel.addColumn(str);
						}

						String[] columns = new String[schema.size()];
						for (int i = 0; i < columns.length; i++) {
							columns[i] = schema.getName(i);
						}

						for (int row = 0; row < response.getResults().getData()
								.size(); row++) {
							Object[] data = new Object[columns.length];

							for (int col = 0; col < columns.length; col++) {
								data[col] = response.getResults().getData()
										.get(row).getField(col);
							}
							m_tablemodel.addRow(data);
						}
					}
				}
			}
		}

		public void handle(BDSaveResponse response, long elapsed) {
			JFileChooser chooser = new JFileChooser();
			int returnVal = chooser.showOpenDialog(BDClient.this);
			if (returnVal == JFileChooser.APPROVE_OPTION) {
				try {
					writeXmlFile(response.getDoc(), chooser.getSelectedFile());
					m_statuslabel.setText("Write database to "
							+ chooser.getSelectedFile().getPath());
				} catch (Exception e) {
					e.printStackTrace();
					m_statuslabel.setText("Could not save database to "
							+ chooser.getSelectedFile().getPath());
				}
			}
		}
	}
	
	static Logger logger = Logger.getLogger(BDClient.class);
	private final static String LOGGER_CONF = "conf/logger.conf";
	private static final long serialVersionUID = 8847929953493642974L;

	public static void main(String[] args) {
		Thread.currentThread().setName("client");

		// Initialize log4j logger
		PropertyConfigurator.configure(LOGGER_CONF);

		new BDClient();
	}
	private JButton m_cancelButton;
	private JButton m_clearButton;
	private JButton m_connectbutton;
	private JTextField m_currentDb;
	private JTextField m_currentTables;
	private JButton m_disconnectbutton;
	private String m_host;
	private JTextField m_hostfield;
	private int m_port;
	private JTextField m_portfield;
	private JProgressBar m_progress;
	private ObjectOutputStream m_requestwriter;
	private ClientResponseHandler m_responseHandler;
	private ObjectInputStream m_responsereader;
	private Map<String, BDSchema> m_schemas;
	private Thread m_serverconn;
	private Socket m_socket;
	private JTextPane m_sqlBox;
	private JLabel m_statuslabel;
	private JButton m_submitbutton;
	private DefaultTableModel m_tablemodel;
    
    private boolean m_isConnected;

	/**
	 * Class constructor.
	 * 
	 * @throws ClientException
	 */
	public BDClient() {
		super("BDSim Client");

		m_responseHandler = new ClientResponseHandler();
        m_isConnected = false;
		
        // Initialize log4j logger
        PropertyConfigurator.configure(LOGGER_CONF);

		this.setDefaultCloseOperation(EXIT_ON_CLOSE);

		try {
			UIManager.setLookAndFeel(new MetalLookAndFeel());
		} catch (Exception e) {
		}
		JFrame.setDefaultLookAndFeelDecorated(true);

		// Add host and port inputs, connect button
		JPanel hostPanel = new JPanel();

		JLabel hostLabel = new JLabel("Host: ");
		hostLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
		hostLabel.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_hostfield = new JTextField();
		this.assignSize(m_hostfield, 110, 20);
		m_hostfield.setText("localhost");
		m_hostfield.setAlignmentX(Component.CENTER_ALIGNMENT);
		m_hostfield.setAlignmentY(Component.CENTER_ALIGNMENT);
		JLabel portLabel = new JLabel("Port: ");
		portLabel.setAlignmentX(Component.CENTER_ALIGNMENT);
		portLabel.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_portfield = new JTextField();
		m_portfield.setText("1877");
		m_portfield.setAlignmentX(Component.CENTER_ALIGNMENT);
		m_portfield.setAlignmentY(Component.CENTER_ALIGNMENT);
		this.assignSize(m_portfield, 50, 20);

		m_connectbutton = new JButton("Connect");
		m_connectbutton.setAlignmentX(Component.CENTER_ALIGNMENT);
		m_connectbutton.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_connectbutton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				new Thread() {
					public void run() {
						connectToHost();
					}
				}.start();
			}
		});
		this.assignSize(m_connectbutton, 90, 30);

		m_disconnectbutton = new JButton("Disconnect");
		m_disconnectbutton.setAlignmentX(Component.CENTER_ALIGNMENT);
		m_disconnectbutton.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_disconnectbutton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				new Thread() {
					public void run() {
						disconnectFromHost();
					}
				}.start();
			}
		});
		this.assignSize(m_disconnectbutton, 110, 30);
		m_disconnectbutton.setEnabled(false);
		
		JLabel currentDb = new JLabel("Current Database:");
		JLabel currentTables = new JLabel("Current Tables:");

		m_currentDb = new JTextField();
		m_currentDb.setEditable(false);
		this.assignSize(m_currentDb, 200, 20);
		m_currentTables = new JTextField();
		m_currentTables.setEditable(false);
		this.assignSize(m_currentTables, 750, 20);
		
		JPanel hostPortBox = new JPanel();
		hostPortBox.setLayout(new BoxLayout(hostPortBox, BoxLayout.X_AXIS));
		hostPortBox.add(hostLabel);
		hostPortBox.add(m_hostfield);
		hostPortBox.add(Box.createHorizontalStrut(8));
		hostPortBox.add(portLabel);
		hostPortBox.add(m_portfield);

		JPanel buttonBox = new JPanel();
		buttonBox.setLayout(new BoxLayout(buttonBox, BoxLayout.X_AXIS));
		buttonBox.setBackground(Color.WHITE);
		buttonBox.add(m_connectbutton);
		buttonBox.add(Box.createHorizontalStrut(10));
		buttonBox.add(m_disconnectbutton);
		
		JPanel databaseFileBox = new JPanel();
		databaseFileBox.setLayout(new BoxLayout(databaseFileBox, BoxLayout.X_AXIS));
		databaseFileBox.setBackground(Color.WHITE);
		databaseFileBox.setAlignmentX(Component.CENTER_ALIGNMENT);
		databaseFileBox.add(currentDb);
		databaseFileBox.add(Box.createHorizontalStrut(10));
		databaseFileBox.add(m_currentDb);

		JPanel tableInfoBox = new JPanel();
		tableInfoBox.setLayout(new BoxLayout(tableInfoBox, BoxLayout.X_AXIS));
		tableInfoBox.setBackground(Color.WHITE);
		tableInfoBox.setAlignmentX(Component.CENTER_ALIGNMENT);
		tableInfoBox.add(currentTables);
		tableInfoBox.add(Box.createHorizontalStrut(10));
		tableInfoBox.add(m_currentTables);

		hostPanel.setLayout(new BoxLayout(hostPanel, BoxLayout.Y_AXIS));
		hostPanel.add(hostPortBox);
		hostPanel.add(buttonBox);

		// Add text input and submit button
		JPanel sqlPanel = new JPanel();

		m_sqlBox = new JTextPane();
		this.assignSize(m_sqlBox, 400, 60);
		m_sqlBox.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_sqlBox.setFont(new Font("SansSerif", Font.PLAIN, 14));
		m_sqlBox.setBorder(BorderFactory.createEtchedBorder());
		m_submitbutton = new JButton("Submit");
		this.assignSize(m_submitbutton, 90, 30);
		m_submitbutton.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_submitbutton.setEnabled(false);
		m_submitbutton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				m_serverconn = new Thread() {
					public void run() {
						makeSqlRequest();
					}
				};
				m_serverconn.start();
			}
		});

		m_cancelButton = new JButton("Cancel");
		this.assignSize(m_cancelButton, 90, 30);
		m_cancelButton.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_cancelButton.setEnabled(false);
		m_cancelButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				if (m_serverconn != null && m_serverconn.isAlive()) {
					m_serverconn.interrupt();
				}
			}
		});

		sqlPanel.setLayout(new BoxLayout(sqlPanel, BoxLayout.X_AXIS));
		sqlPanel.add(m_sqlBox);
		sqlPanel.add(Box.createHorizontalStrut(30));
		sqlPanel.add(m_submitbutton);
		sqlPanel.add(Box.createHorizontalStrut(8));
		sqlPanel.add(m_cancelButton);

		// Add table
		m_tablemodel = new DefaultTableModel();
		JTable table = new JTable(m_tablemodel) {
			private static final long serialVersionUID = -3507984152006993963L;

			public boolean isCellEditable(int rowIndex, int vColIndex) {
				return false;
			}
		};

		table.setColumnSelectionAllowed(true);
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.setRowHeight(14);
		table.setShowGrid(true);

		JScrollPane scrollPane = new JScrollPane(table);
		JPanel scrollPanePanel = new JPanel();
		scrollPanePanel.setLayout(new BoxLayout(scrollPanePanel,
				BoxLayout.Y_AXIS));
		scrollPanePanel.add(Box.createVerticalStrut(8));
		scrollPanePanel.add(scrollPane);
		scrollPanePanel.add(Box.createVerticalStrut(8));

		// Add progress bar and status bar
		JPanel southPanel = new JPanel();

		m_progress = new JProgressBar();
		this.assignSize(m_progress, 300, 20);
		m_progress.setOrientation(JProgressBar.HORIZONTAL);
		m_progress.setBackground(Color.WHITE);
		m_progress.setForeground(Color.RED);
		m_progress.setBorderPainted(true);

		m_statuslabel = new JLabel();

		m_clearButton = new JButton("Clear");
		this.assignSize(m_clearButton, 80, 30);
		m_clearButton.setAlignmentY(Component.CENTER_ALIGNMENT);
		m_clearButton.setEnabled(false);
		m_clearButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				removeAllData();
			}
		});

		southPanel.setLayout(new BoxLayout(southPanel, BoxLayout.X_AXIS));
		southPanel.add(m_progress);
		southPanel.add(Box.createHorizontalStrut(20));
		southPanel.add(m_clearButton);
		southPanel.add(Box.createHorizontalStrut(20));
		southPanel.add(m_statuslabel);

		// Add all components to main panel
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());

		JPanel northSqlPanel = new JPanel();
		northSqlPanel.setLayout(new BoxLayout(northSqlPanel, BoxLayout.X_AXIS));

		northSqlPanel.add(hostPanel);
		northSqlPanel.add(Box.createHorizontalStrut(30));
		northSqlPanel.add(sqlPanel);
		
		JPanel northPanel = new JPanel();
		northPanel.setLayout(new BoxLayout(northPanel, BoxLayout.Y_AXIS));

		northPanel.add(northSqlPanel);
		northPanel.add(Box.createVerticalStrut(10));
		northPanel.add(databaseFileBox);
		northPanel.add(Box.createVerticalStrut(5));
		northPanel.add(tableInfoBox);
		northPanel.add(Box.createVerticalStrut(5));

		mainPanel.add(northPanel, BorderLayout.NORTH);
		mainPanel.add(southPanel, BorderLayout.SOUTH);
		mainPanel.add(scrollPanePanel, BorderLayout.CENTER);

		mainPanel.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
		this.setContentPane(mainPanel);
		int sizeFromEdge = 100;
		Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
		Dimension newDim = new Dimension(dim.width - (2 * sizeFromEdge),
				dim.height - (2 * sizeFromEdge));
		this.setLocation(sizeFromEdge, sizeFromEdge);
		this.setSize(newDim);

		// Add menubar to frame
		this.setJMenuBar(this.constructMenuBar());
		
		// Set colors for Swing components
		northSqlPanel.setBackground(Color.WHITE);
		northPanel.setBackground(Color.WHITE);
		mainPanel.setBackground(Color.WHITE);
		southPanel.setBackground(Color.WHITE);
		m_sqlBox.setBackground(Color.WHITE);
		sqlPanel.setBackground(Color.WHITE);
		hostPortBox.setBackground(Color.WHITE);
		buttonBox.setBackground(Color.WHITE);
		hostPanel.setBackground(Color.WHITE);
		scrollPanePanel.setBackground(Color.WHITE);

		this.setVisible(true);
	}

	/**
	 * Assigns a specific size to a <code>Component</code> by calling
	 * <code>setMinimumSize</code> and <code>setMaximumSize</code>
	 * 
	 * @param comp
	 *            The <code>Component</code> to adjust
	 * @param width
	 *            The width to assign
	 * @param height
	 *            The height to assign
	 */
	private void assignSize(Component comp, int width, int height) {
		comp.setMaximumSize(new Dimension(width, height));
		comp.setMinimumSize(new Dimension(width, height));
		comp.setSize(new Dimension(width, height));
		comp.setPreferredSize(new Dimension(width, height));
	}

	/**
	 * Connects to the host, specified by the host and port parameters.
	 */
	private void connectToHost() {
		m_progress.setIndeterminate(true);
		String error = "";

        m_statuslabel.setText("Trying to connect to "
				+ m_hostfield.getText().trim() + ":"
				+ m_portfield.getText().trim() + "...");
        
		try {
			this.m_port = Integer.parseInt(m_portfield.getText().trim());
			this.m_host = m_hostfield.getText().trim();
			this.m_socket = new Socket(m_host, m_port);
			this.m_requestwriter = new ObjectOutputStream(m_socket
					.getOutputStream());
			this.m_responsereader = new ObjectInputStream(m_socket
					.getInputStream());
		} catch (NumberFormatException e) {
			error = "Illegal port number: " + m_portfield.getText();
		} catch (UnknownHostException e) {
			error = "Unknown host: " + m_hostfield.getText();
		} catch (IOException e) {
			error = "Error: " + e.getMessage();
		}

		m_progress.setIndeterminate(false);

		if (error.equals("")) {
			m_statuslabel.setText("Connected to host "
					+ m_socket.getRemoteSocketAddress());
			m_connectbutton.setEnabled(false);
			m_disconnectbutton.setEnabled(true);
			m_submitbutton.setEnabled(true);
			m_hostfield.setEnabled(false);
			m_portfield.setEnabled(false);
            m_isConnected = true;
			
			makeRequest(new BDInfoRequest());
		} else {
			m_statuslabel.setText(error);
            m_isConnected = false;
		}
	}

	private JMenuBar constructMenuBar() {
		JMenuBar menuBar = new JMenuBar();
		
		JMenu fileMenu = new JMenu("File");
        
		JMenuItem runScript = new JMenuItem("Run SQL Script");
        runScript.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to run a script.");
					return;
				}

				JFileChooser chooser = new JFileChooser();
				int returnVal = chooser.showOpenDialog(BDClient.this);
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					File file = chooser.getSelectedFile();
					logger.debug("Reading script file " + file.getAbsolutePath());
                    try {
                        BufferedReader breader = new BufferedReader(new FileReader(file));
                        String line = "";
                        
                        while(null != (line = breader.readLine())) {
                            logger.debug("Making request with SQL: " + line);
                            makeRequest(new BDSqlRequest(line));
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
        fileMenu.add(runScript);
        fileMenu.add(new JSeparator());
		JMenuItem createTable = new JMenuItem("Create Table");
		createTable.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to create a table.");
					return;
				}                
				// TODO
			}			
		});
		fileMenu.add(createTable);
		JMenuItem dropTable = new JMenuItem("Drop Table");
		dropTable.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {				
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to drop a table.");
					return;
				}
				Object[] possibilities = m_schemas.keySet().toArray();
				String tableName = (String) JOptionPane.showInputDialog(BDClient.this,
						"Select a table to drop:", "Drop Table",
						JOptionPane.QUESTION_MESSAGE, null, possibilities,
						"ham");
				if(tableName != null && tableName.length() > 0) {
					makeRequest(new BDDropRequest(tableName));
				} 
			}
		});
		fileMenu.add(dropTable);
		fileMenu.add(new JSeparator());
		JMenuItem loadXml = new JMenuItem("Load XML");
		loadXml.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to load an XML file.");
					return;
				}
                
                JFileChooser chooser = new JFileChooser();
				int returnVal = chooser.showOpenDialog(BDClient.this);
				if (returnVal == JFileChooser.APPROVE_OPTION) {
					try {
						DocumentBuilderFactory factory = DocumentBuilderFactory
								.newInstance();
						factory.setValidating(false);
						Document doc = factory.newDocumentBuilder().parse(
								chooser.getSelectedFile());
						makeRequest(new BDLoadRequest(doc, chooser
								.getSelectedFile().getName()));
					} catch (SAXException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParserConfigurationException e) {
						e.printStackTrace();
					}
				}
			}
		});
		fileMenu.add(loadXml);
		JMenuItem saveXml = new JMenuItem("Save XML");
		saveXml.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to save an XML file.");
					return;
				}
                makeRequest(new BDSaveRequest());
			}			
		});
		fileMenu.add(saveXml);		
		fileMenu.add(new JSeparator());
		JMenuItem quit = new JMenuItem("Quit");
		quit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				logger.debug("Exiting BDClient...");
				System.exit(0);
			}			
		});
		fileMenu.add(quit);
		
		JMenu helpMenu = new JMenu("Help");
		JMenuItem showDetailsItem = new JMenuItem("Show Table Details");
		showDetailsItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
    			if(!m_isConnected) {
                    m_statuslabel
							.setText("You must be connected to a database to show the table details.");
					return;
				}
                // TODO
			}			
		});
		helpMenu.add(showDetailsItem);
        helpMenu.add(new JSeparator());
		JMenuItem about = new JMenuItem("About");
		about.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				// TODO
			}			
		});
		helpMenu.add(about);
		
		menuBar.add(fileMenu);
		menuBar.add(helpMenu);
		
		return menuBar;		
	}

	/**
	 * Disconnects from the current server.
	 */
	private void disconnectFromHost() {
		try {
			m_responsereader.close();
			m_requestwriter.close();
			m_socket.close();
		} catch (IOException e) {
            m_isConnected = false;
            return;
		}

		m_responsereader = null;
		m_requestwriter = null;
        m_isConnected = false;

		m_statuslabel.setText("Disconnect from host " + m_host + ":" + m_port);
		m_sqlBox.setText("");
		m_connectbutton.setEnabled(true);
		m_disconnectbutton.setEnabled(false);
		m_submitbutton.setEnabled(false);
		m_hostfield.setEnabled(true);
		m_portfield.setEnabled(true);
	}

	/**
	 * Sends a request over to the server.
	 * @param request  The request to send to the server
	 */
	private void makeRequest(BDRequest request) {
		try {
			m_requestwriter.writeObject(request);
			m_progress.setIndeterminate(true);

			long start = System.currentTimeMillis();
			BDResponse response = (BDResponse) (m_responsereader.readObject());
			long elapsed = System.currentTimeMillis();

			response.handle(m_responseHandler, elapsed - start);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			m_clearButton.setEnabled(true);
			m_cancelButton.setEnabled(false);
			m_disconnectbutton.setEnabled(true);
			m_progress.setIndeterminate(false);
			m_submitbutton.setEnabled(true);
		}
	}

	/**
	 * Makes a request to the database server.
	 */
	private void makeSqlRequest() {
		if (!m_sqlBox.getText().equals("")) {
			m_cancelButton.setEnabled(true);
			m_disconnectbutton.setEnabled(false);
			m_submitbutton.setEnabled(false);

			BDRequest request = new BDSqlRequest(m_sqlBox.getText());
			makeRequest(request);
		} else {
			m_statuslabel.setText("Please specify SQL to submit");
		}
	}
	
	/**
	 * Removes all listings from the table.
	 */
	private void removeAllData() {
		m_tablemodel.setNumRows(0);
		m_tablemodel.setColumnCount(0);
		m_clearButton.setEnabled(false);
	}
	
	/**
	 * Takes an existing Document and writes it to file.
	 * 
	 * @param doc
	 *            The document of the file to write
	 * @param file
	 *            The path to write the XML file
	 * @throws XmlException
	 */
	private void writeXmlFile(Document doc, File file)
			throws Exception {
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			// Prepare the output file
			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getLocalizedMessage());
		}
	}
}
