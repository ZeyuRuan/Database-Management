package bdsim.server.system;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import bdsim.server.system.index.BDBPlusTreeVisualizer;
import bdsim.server.main.BDSimulator;

/**
 * Manages all of the tables in the system, and allows the system to swap an XML
 * representation of a database out to file and back in again.
 * 
 * @author dclee
 * @author wpijewsk
 * @revision $Id: BDTableManager.java 301 2007-01-22 18:32:38 +0000 (Mon, 22 Jan 2007) acath $
 */
public final class BDTableManager {

    public class XmlException extends Exception {

		private static final long serialVersionUID = 3690480229403669554L;

		public XmlException(String string) {
			super(string);
		}
	}
	
	static Logger logger = Logger.getLogger(BDTableManager.class);

	// TODO Bill needs methods to call to get metadata for the planner. Maybe
	// this is a good place?
	private String m_fileName;
	
	private HashMap<String, BDTable> m_tables;

	public BDTableManager() {
		m_tables = new HashMap<String, BDTable>();
	}

	public void createTable(String s, BDTable t) {
		m_tables.put(s, t);
		if ("true".equals(System.getProperty("visualizer.doShow"))) {
			BDBPlusTreeVisualizer v = new BDBPlusTreeVisualizer(s, t);
			v.setSize(1024, 300);
			v.setVisible(true);
			//
			// Pavlo Hack!
			//
			if (s.equals("Customers")) {
				BDSimulator.visualizer = v;
			}
		}
	}

	public void dropTable(String s) {
		m_tables.remove(s);
	}

	/**
	 * Saves the current database state in a Document element.
	 * 
	 * @return  The current state of the database in a 
	 * @throws XmlException
	 */
	public Document getCurrentDatabase() throws XmlException {
		// TODO Need to lock the database here
		
		Document doc = null;
		try {
			doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
					.newDocument();

			// Insert the root element node
			Element rootElement = doc.createElement("Database");
			doc.appendChild(rootElement);

			for (String tableName : m_tables.keySet()) {
				this.writeTable(doc, rootElement, tableName);
			}

		} catch (Exception e) {
			e.printStackTrace();
			throw new XmlException(e.getLocalizedMessage());
		} 
		return doc;
	}

	public String getFileName() {
		return m_fileName;
	}

	public BDTable getTableByName(String s) {
		return m_tables.get(s);
	}
	
	public Map<String, BDTable> getTables() {
		return m_tables;
	}

	public boolean isTable(String s) {
		return m_tables.containsKey(s);
	}
	
	/**
	 * Reads a database from a Document into the current table manager.
	 * 
	 * @param doc
	 *            The Document from which to read
	 */
	public void readFromXmlFile(Document doc) {
    	//TODO wpijewsk make sure to block rest of transactions
    	// in the scheduler to load new set of data
		
        m_tables = null;
        m_tables = new HashMap<String, BDTable>();
        
        Element rootElement = doc.getDocumentElement();

		for (int i = 0; i < rootElement.getChildNodes().getLength(); i++) {
			Node childElement = rootElement.getChildNodes().item(i);
			readTable(childElement);
		}
	}
    
    /**
	 * Reads a database from an XML file into the current table manager.
	 * 
	 * @param xmlFile
	 *            The XML file from which to read
	 * @throws XmlException
	 */
	public void readFromXmlFile(String xmlFile) throws XmlException {
        try {
            // Create the builder and parse the file
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setValidating(false);
            Document doc = factory.newDocumentBuilder().parse(new File(xmlFile));
            
            this.readFromXmlFile(doc);
			
			m_fileName = xmlFile;
		} catch (Exception e) {
			e.printStackTrace();
			throw new XmlException(e.getLocalizedMessage());
		}
	}

	/**
	 * Reads a BDTable XML element and adds its data to the current BDTableManager.
	 * 
	 * @param tableElement
	 *            The BDTable <code>Node</code> to add to this table manager
	 */
	private void readTable(Node tableElement) {
        
		if(!(tableElement instanceof Element)) {
			return;
		}
		
		NamedNodeMap tableAttributes = tableElement.getAttributes();
		String name = null;
		name = tableAttributes.getNamedItem("Name").getNodeValue();
		
        Node schemaNode = tableElement.getFirstChild();
        Node dataNode = tableElement.getLastChild();

		Vector<String> names = new Vector<String>();
		Vector<BDObjectType> types = new Vector<BDObjectType>();
		
		String primaryIndex = null;
		List<String> secondaryIndices = new LinkedList<String>();

		for (int i = 0; i < schemaNode.getChildNodes().getLength(); i++) {
			Node fieldNode = schemaNode.getChildNodes().item(i);
			NamedNodeMap fieldattributes = fieldNode.getAttributes();
			names.add(fieldattributes.getNamedItem("Name").getNodeValue());
			BDObjectType type = BDObjectType.convertTo(fieldattributes.getNamedItem(
					"Type").getNodeValue());
			types.add(type);
			if (fieldattributes.getNamedItem("Index") != null) {
				if (fieldattributes.getNamedItem("Index").getNodeValue().equals(
						"PRIMARY")) {
					primaryIndex = fieldattributes.getNamedItem("Name")
							.getNodeValue();
				} else if (fieldattributes.getNamedItem("Index").getNodeValue()
						.equals("SECONDARY")) {
					secondaryIndices.add(fieldattributes.getNamedItem("Name")
							.getNodeValue());
				}
			}
		}

		logger.debug("Building a new table with primary index " + primaryIndex);
		
		BDSchema newSchema = new BDSchema(names, types);
		BDTable newTable = new BDTable(newSchema, primaryIndex, name);
		for(String secIndex : secondaryIndices) {
			newTable.buildIndexOnAttribute(secIndex);
		}

		for (int i = 0; i < dataNode.getChildNodes().getLength(); i++) {
			Node tupleNode = dataNode.getChildNodes().item(i);
			NamedNodeMap tupleAttributes = tupleNode.getAttributes();
			BDTuple newTuple = new BDTuple(newSchema);

			for (String colName : newSchema.getNames()) {
				Object dataItem = null;
				String strItem = tupleAttributes.getNamedItem(colName)
						.getNodeValue();

				try {
					Double doubleItem = Double.parseDouble(strItem);
					dataItem = doubleItem;
				} catch (NumberFormatException e) {
					dataItem = strItem;
				}

				newTuple.setObject(colName, dataItem);
			}
			try {
				newTable.insertUnsafe(newTuple);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		this.createTable(name, newTable);
	}

	/**
	 * Writes a single table to the current document element.
	 * 
	 * @param doc
	 * @param element
	 * @param tableName
	 * @throws XmlException
	 */
	private void writeTable(Document doc, Element element, String tableName)
			throws XmlException {
		BDTable table = m_tables.get(tableName);

		if (table == null) {
			throw new XmlException("Table " + tableName + " does not exist.");
		}

		BDSchema curSchema = table.getSchema();

		Element tableElement = doc.createElement("Table");
		tableElement.setAttribute("Name", tableName);

		Element schemaElement = doc.createElement("Schema");
		Element dataElement = doc.createElement("Data");

		element.appendChild(tableElement);
		tableElement.appendChild(schemaElement);
		tableElement.appendChild(dataElement);

		for (String s : curSchema.getNames()) {
			Element tableNameElement = doc.createElement("Field");
			schemaElement.appendChild(tableNameElement);

			tableNameElement.setAttribute("Name", s);
			tableNameElement.setAttribute("Type", curSchema.getObjectType(
					curSchema.getPosition(s)).toString());
			if (table.isPrimaryIndex(s)) {
				tableNameElement.setAttribute("Index", "PRIMARY");
			} else if (table.isSecondaryIndex(s)) {
				tableNameElement.setAttribute("Index", "SECONDARY");
			}
		}

		List<BDTuple> allTuples = table.getAllTuplesUnchecked().getTupleData();

		// Make a new element for each tuple in the table
		for (BDTuple t : allTuples) {
			Element rowElement = doc.createElement("Tuple");
			dataElement.appendChild(rowElement);

			for (String columnName : curSchema.getNames()) {
				rowElement.setAttribute(columnName, t.getField(columnName)
						.toString());
			}
		}
	}
	
	/**
	 * Writes this schema and all of the the data in this table manager to an
	 * XML file.
	 * 
	 * @param xmlFile
	 *            The XML file to write
	 * @throws XmlException
	 */
	public void writeToXmlFile(String xmlFile) throws XmlException {
		try {
			Document doc = this.getCurrentDatabase();
			writeXmlFile(doc, xmlFile);
		} catch (Exception e) {
			e.printStackTrace();
			throw new XmlException(e.getLocalizedMessage());
		}
	}

	/**
	 * Takes an existing Document and writes it to file.
	 * 
	 * @param doc
	 *            The document of the file to write
	 * @param filename
	 *            The path to write the XML file
	 * @throws XmlException
	 */
	private void writeXmlFile(Document doc, String filename)
			throws XmlException {
		try {
			// Prepare the DOM document for writing
			Source source = new DOMSource(doc);

			// Prepare the output file
			File file = new File(filename);
			Result result = new StreamResult(file);

			// Write the DOM document to the file
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new XmlException(e.getLocalizedMessage());
		}
	}
}
