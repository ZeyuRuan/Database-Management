package bdsim.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * Generates XML files for use in the BDSim database system. Takes a CSV file of
 * data and converts it to an XML file that can be loaded as a database.
 * 
 * @author wpijewsk
 * @revision $Id: BDDatabaseGenerator.java 216 2007-01-17 16:52:03 +0000 (Wed, 17 Jan 2007) wpijewsk $
 */
public final class BDDatabaseGenerator {

	// TODO Info option   
	
	private class DatabaseGeneratorException extends Exception {
		private static final long serialVersionUID = 2405412858520467698L;

		public DatabaseGeneratorException(String string) {
			super(string);
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2 || args[0].equals("-h")) {
			System.out.println("To create a new database: -n <XML file>");
			System.out
					.println("To add a new table: -a <XML file> <CSV file> <BDTable name>");
			System.out.println("To delete a table: -d <XML file> <BDTable name>");
			System.out.println("For information about a database: -i <XML file>");
			System.exit(1);
		}
		BDDatabaseGenerator generator = new BDDatabaseGenerator();

		try {
			if (args[0].equals("-a")) {
				if (args.length < 4) {
					System.out
							.println("To add a new table: -a <XML file> <CSV file> <BDTable name>");
					System.exit(1);
				}
				generator.addTable(args[1], args[2], args[3]);
				System.out.println("Added table " + args[3] + " to database "
						+ args[1]);
			} else if (args[0].equals("-d")) {
				if (args.length < 3) {
					System.out
							.println("To delete a table: -d <XML file> <BDTable name>");
					System.exit(1);
				}
				generator.deleteTable(args[1], args[2]);
				System.out.println("Removed " + args[2] + " from database "
						+ args[1]);
			} else if (args[0].equals("-n")) {
				if (args.length < 2) {
					System.out
							.println("To create a new database: -n <XML file>");
					System.exit(1);
				}
				generator.createDatabase(args[1]);
				System.out.println("Created database " + args[1]);
			}else if (args[0].equals("-i")) {
				if (args.length < 2) {
					System.out
							.println("For information about a database: -i <XML file>");
					System.exit(1);
				}				
				generator.displayInformation(args[1]);
			} else {
				System.out.println("Illegal option: " + args[0]);
				System.out.println("To create a new database: -n <XML file>");
				System.out
						.println("To add a new table: -a <XML file> <CSV file> <BDTable name>");
				System.out
						.println("To delete a table: -d <XML file> <BDTable name>");
				System.out.println("For information about a database: -i <XML file>");
				System.exit(1);
			}
		} catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
			e.printStackTrace();
			System.exit(1);
		}
	}
    
	/**
	 * Displays the schema of an XML database.
	 * 
	 * @param filePath
	 * @throws DatabaseGeneratorException
	 */
	private void displayInformation(String filePath)
			throws DatabaseGeneratorException {
		try {
			File xmlFile = new File(filePath);

			if (!xmlFile.exists()) {
				System.out.println("Cannot open file " + filePath);
			} else {
				DocumentBuilderFactory factory = DocumentBuilderFactory
						.newInstance();
				factory.setValidating(false);
				Document doc = factory.newDocumentBuilder().parse(xmlFile);
				Element rootElement = doc.getDocumentElement();
				for (int i = 0; i < rootElement.getChildNodes().getLength(); i++) {
					Element tableElement = (Element) rootElement
							.getChildNodes().item(i);
					NamedNodeMap tableAttributes = tableElement.getAttributes();
					System.out.println("Table "
							+ tableAttributes.getNamedItem("Name"));
					Element fieldsElement = (Element) tableElement
							.getFirstChild();
					for (int j = 0; j < fieldsElement.getChildNodes()
							.getLength(); j++) {
						Element fieldElement = (Element) fieldsElement
								.getChildNodes().item(j);
						NamedNodeMap fieldAttributes = fieldElement
								.getAttributes();
						System.out.print("\t"+ fieldAttributes.getNamedItem("Name") + "\t");
						System.out.print(fieldAttributes.getNamedItem("Type") + "\t");
						if (fieldAttributes.getNamedItem("Index") != null) {
							System.out.print(fieldAttributes.getNamedItem("Index"));
						}
						System.out.print("\n");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseGeneratorException(e.getLocalizedMessage());
		}
	}

	/**
	 * Opens an XML file (creating it if it doesn't already exist), opens the
	 * CSV file, converts that CSV file to an XML element, and appends that
	 * element to the database file.
	 * 
	 * @throws DatabaseGeneratorException
	 */
	private void addTable(String origFilePath, String csvFilePath, String tableName)
			throws DatabaseGeneratorException {
		try {
			// Open XML file and get root element
			File origFile = new File(origFilePath);
			Document doc = null;
			Node rootElement = null;
			Element tableElement = null;
			if (new File(origFilePath).exists()) {
				DocumentBuilderFactory factory = DocumentBuilderFactory
						.newInstance();
				factory.setValidating(false);
				doc = factory.newDocumentBuilder().parse(origFile);
				rootElement = doc.getDocumentElement();
			} else {
				if (!origFile.createNewFile()) {
					throw new DatabaseGeneratorException("Cannot create file "
							+ origFilePath);
				}
				doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
						.newDocument();
				rootElement = doc.createElement("Database");
				doc.appendChild(rootElement);
			}
			tableElement = doc.createElement("Table");
			rootElement.appendChild(tableElement);
			tableElement.setAttribute("Name", tableName);

			// Open CSV file
			File csvFile = new File(csvFilePath);
			if (!csvFile.exists()) {
				throw new DatabaseGeneratorException(csvFilePath
						+ " does not exist!");
			}

			addTableToNode(doc, tableElement, new FileReader(csvFile));

			// Write the DOM file to disk
			Source source = new DOMSource(doc);
			Result result = new StreamResult(origFile);
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseGeneratorException(e.getLocalizedMessage());
		}
	}

    private void addTableToNode(Document doc, Node tableElement,
			Reader reader) throws DatabaseGeneratorException {

		try {
			BufferedReader breader = new BufferedReader(reader);

			Node schemaElement = doc.createElement("Fields");
			Node dataElement = doc.createElement("Data");
			tableElement.appendChild(schemaElement);
			tableElement.appendChild(dataElement);

			Vector<String> names = new Vector<String>();
			Vector<String> types = new Vector<String>();
			Vector<String> indices = new Vector<String>();

			String str = breader.readLine();
			String[] nameTokens = str.split(",");
			str = breader.readLine();
			String[] typeTokens = str.split(",");
			str = breader.readLine();
			String[] indexTokens = str.split(",");

			if (nameTokens.length != typeTokens.length
					|| typeTokens.length != indexTokens.length) {
				throw new DatabaseGeneratorException(
						"Illegal file format: not same number of columns in each header row");
			}

			for (int i = 0; i < nameTokens.length; i++) {
				names.add(i, removeQuotes(nameTokens[i]));
				types.add(i, removeQuotes(typeTokens[i]));
				indices.add(i, removeQuotes(indexTokens[i]));
			}

			for (int i = 0; i < nameTokens.length; i++) {
				Element fieldElement = doc.createElement("Field");
				schemaElement.appendChild(fieldElement);
				fieldElement.setAttribute("Name", names.get(i));
				fieldElement.setAttribute("Type", types.get(i));
				if (indices.get(i) != null && !indices.get(i).equals("NONE")) {
					fieldElement.setAttribute("Index", indices.get(i));
				}
			}

			while (null != (str = breader.readLine())) {
				String[] dataTokens = str.split(",");
				if (dataTokens.length != nameTokens.length) {
					throw new DatabaseGeneratorException(
							"Illegal file format: not same number of columns in each data row");

				}
				Element tupleElement = doc.createElement("Data");
				dataElement.appendChild(tupleElement);
				for (int i = 0; i < nameTokens.length; i++) {
					tupleElement.setAttribute(names.get(i),
							removeQuotes(dataTokens[i]));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseGeneratorException(e.getLocalizedMessage());
		}
	}
    
	private void createDatabase(String origFilePath) throws DatabaseGeneratorException {
		File origFile = new File(origFilePath);
		
		try {
			if (!origFile.createNewFile()) {
				throw new DatabaseGeneratorException("Cannot create file "
						+ origFilePath);
			}
			Document doc = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().newDocument();
			Element rootElement = doc.createElement("Database");
			doc.appendChild(rootElement);
			
            // Write the DOM file to disk
			Source source = new DOMSource(doc);
			Result result = new StreamResult(origFile);
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseGeneratorException(e.getLocalizedMessage());
		}
    }

	/**
	 * Deletes a table from a database.
	 * 
	 * @param origFilePath
	 *            The file path of the XML file to delete the table from
	 * @param tableName
	 *            The name of the table to delete.
	 * @throws DatabaseGeneratorException
	 */
	private void deleteTable(String origFilePath, String tableName)
			throws DatabaseGeneratorException {
		try {
			// Open XML file and get root element
			File origFile = new File(origFilePath);
			Document doc = null;
			Element rootElement = null;
			if (new File(origFilePath).exists()) {
				DocumentBuilderFactory factory = DocumentBuilderFactory
						.newInstance();
				factory.setValidating(false);
				doc = factory.newDocumentBuilder().parse(origFile);
				rootElement = doc.getDocumentElement();

				for (int i = 0; i < rootElement.getChildNodes().getLength(); i++) {
					Element tableElement = (Element) rootElement
							.getChildNodes().item(i);
					if (tableElement.getAttribute("Name").equalsIgnoreCase(
							tableName)) {
						rootElement.removeChild(tableElement);
					}
				}
			} else {
				throw new DatabaseGeneratorException(origFilePath + " does not exist");
			}

			// Write the DOM file to disk
			Source source = new DOMSource(doc);
			Result result = new StreamResult(origFile);
			Transformer xformer = TransformerFactory.newInstance()
					.newTransformer();
			xformer.transform(source, result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DatabaseGeneratorException(e.getLocalizedMessage());
		}
    }

	private String removeQuotes(String string) {
		if(string == null || string.length() == 0) {
			return "";
		}
		
		if (string.charAt(0) == '"' || string.charAt(0) == '\'') {
			string = string.substring(1);
		}
		if (string.charAt(string.length() - 1) == '"'
				|| string.charAt(string.length() - 1) == '\'') {
			string = string.substring(0, string.length() - 1);
		}

		return string;
	}
}
