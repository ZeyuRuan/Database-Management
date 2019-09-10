package bdsim.server.system.index;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.File;
import java.util.*;

import junit.framework.TestCase;
import bdsim.server.system.BDTable;
import bdsim.server.system.BDTableManager;
import bdsim.server.system.BDTuple;
import bdsim.server.system.BDTableManager.XmlException;

public class BDBPlusTreeIndexStudentTest extends TestCase {

	protected void setUp() throws Exception {
		super.setUp();
		FileInputStream propFile = null;
        Properties p = null;
        try {
            propFile = new FileInputStream("conf/server.conf");
            p = new Properties(System.getProperties());
            p.load(propFile);
            System.setProperties(p);
            propFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
		
		System.setProperty("visualizer.doShow", "false");
		BDTableManager t_manager = new BDTableManager();
		t_manager.readFromXmlFile("test/files/Bank.xml-EMPTY");
		customers_table = t_manager.getTableByName("Customers");
		m_d = 2;
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	
	private int m_d;
	private BDBPlusTreeIndex index;
	private LineNumberReader m_script;
	private StringBuilder m_stdout;
	private StringBuilder m_stderr;	
	private BDTable customers_table;
	
	/**
	 * Checks to make sure that the tree starting at a given node is structrually valid
	 * 
	 * @param parent the starting node to examine the tree
	 * @param pMin the min value for the sub-tree rooted at the parent (can be null)
	 * @param pMax the max value for the sub-tree rooted at the parent (can be null)
	 */
	protected void checkTree(BDBPlusTreeNode parent, Comparable pMin, Comparable pMax) {
		int half = (int)Math.floor(this.m_d / 2.0);
		
		//
		// Check that the values of the root of the tree is in proper order
		//
		if (parent == index.getTree().getTreeRoot()) {
			Comparable lastValue = null;
			for (int ctr = 0; ctr < parent.keyCount(); ctr++) {
				if (lastValue != null && parent.getKey(ctr).compareTo(lastValue) <= 0) {
					try {
						throw new Exception("ERROR: Root Node #" + parent.getId() + " has invalid sorted values!");
					} catch (Exception ex) {
						ex.printStackTrace();
						this.debugNode(parent, "Invalid Node Values ", -1);
						fail();
					}
				}
				lastValue = parent.getKey(ctr);
			}
		}
		
		for (int child_ctr = 0; child_ctr < parent.childCount(); child_ctr++) {
			BDBPlusTreeNode child = parent.getChild(child_ctr);
			
			//
			// Make sure the tree is balanced properly
			//
			if ((child.isLeaf() && child.keyCount() < half) || (!child.isLeaf() && child.childCount() < half)) {
				this.debugNode(index.getTree().getTreeRoot(), "Root ");
				try {
					throw new Exception("ERROR: Parent #" + parent.getId() + " Child #" + child.getId());
				} catch (Exception ex) {
					ex.printStackTrace();
					System.err.println("CHILD HAS TOO FEW ITEMS!");
					System.err.println("childCount = " + child.childCount());
					System.err.println("valueCount = " + child.keyCount());
					System.err.println("MIN AMOUNT = " + half);
					fail();
				}
			}
			//
			// Make sure the values in this node are in the right order
			//
			Comparable lastValue = null;
			for (int ctr = 0; ctr < child.keyCount(); ctr++) {
				if (lastValue != null && child.getKey(ctr).compareTo(lastValue) <= 0) {
					try {
						throw new Exception("ERROR: Node #" + child.getId() + " has invalid sorted values!");
					} catch (Exception ex) {
						ex.printStackTrace();
						this.debugNode(child, "Invalid Node Values ", -1);
						fail();
					}
				}
				lastValue = child.getKey(ctr);
			}
			
			for (int ctr = 0; ctr < child.keyCount(); ctr++) {
				Comparable value = child.getKey(ctr);
				boolean fail = false;
				Comparable min = (child_ctr == 0 ? pMin : parent.getKey(child_ctr - 1));
				Comparable max = (child_ctr == parent.keyCount() ? pMax : parent.getKey(child_ctr));
				
				//System.err.println((min == null ? "null" : min) + " <= " + value + " < " + (max == null ? "null" : max));
				
				if (min != null && value.compareTo(min) < 0) {
					fail = true;
				}
				if (max != null && value.compareTo(max) >= 0) {
					fail = true;
				}
				if (fail) {
					this.debugNode(index.getTree().getTreeRoot(), "Troubled Parent ");
					//this.debugNode(parent, "Troubled Parent ");
					try {
						throw new Exception("ERROR: Parent #" + parent.getId() + " Child #" + child.getId());
					} catch (Exception ex) {
						ex.printStackTrace();
						System.err.println("Value = " + value);
						System.err.println("Min   = " + min);
						System.err.println("Max   = " + max);
						//System.exit(-1);
						fail();
					}
				}
				if (!child.isLeaf()) {
					this.checkTree(child, min, max);
				}
			}
			
		}
		
	}
	
	/**
	 * Prints out a debug statement for a given node
	 * 
	 * @param node the node print out the children and values for
	 * @param str the string to include in the title
	 */
	protected void debugNode(BDBPlusTreeNode node, String str) {
		this.debugNode(node, str, 0);
	}

	/**
	 * Prints out a debug statement for a given node with proper tab spacing
	 * based on the level of the tree
	 * 
	 * @param node the node print out the children and values for
	 * @param str the string to include in the title
	 * @param tab how many levels deep we are in the tree for node
	 */
	protected void debugNode(BDBPlusTreeNode node, String str, int tab) {
		String tabstr = "";
		for (int ctr = 0; ctr <= tab; ctr++) {
			tabstr += "   ";
		}
		System.err.println((tab == 0 ? "PAVLO: " : tabstr) + str + "Node #" + node.getId());
		try {				
			if (!node.isLeaf()) {
				System.err.print((tab == 0 ? "" : tabstr) + "[");
				for (int idx = 0; idx < node.childCount(); idx++) {
					if (idx == 0) {
						System.err.print("#" + node.getChild(idx).getId());
					} else {
						System.err.print("|" + node.getKey(idx - 1) + "|#" + node.getChild(idx).getId());
					}

					// We might have an extra key: make sure to print it out
					if (idx == node.childCount() - 1) {
						for (int idx2 = idx; idx2 < node.keyCount(); ++idx2) {
							System.err.print("|" + node.getKey(idx2));
						}
					}
				}
				System.err.println("]");
				if (tab >= 0) {
					for (int idx = 0; idx < node.childCount(); idx++) {
						this.debugNode(node.getChild(idx), "", tab + 1);
					}
				}
			} else {
				for (int idx = 0; idx < node.keyCount(); idx++) {
					System.err.println((tab == 0 ? "" : tabstr) + "   " + idx + ") " + 
										node.getKey(idx) + " --> " + node.getTuple(idx));
					
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return;
	}

	private void doOperation(String opcode, String arg, int line) {
		Double key = new Double(Integer.parseInt(arg));
		BDTuple tuple = new BDTuple(customers_table.getSchema());
		tuple.setObject(0, key);
		tuple.setObject(1, "Customer First Name");
		tuple.setObject(2, "Customer Last Name");
		tuple.setObject(3, "Customer SSN");
		if ("insert".equals(opcode)) {
			try {
				index.getTree().insert(tuple);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			checkTree(index.getTree().getRoot(), null, null);
		} else if ("delete".equals(opcode)) {
			try {
				index.getTree().delete(tuple);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			checkTree(index.getTree().getRoot(), null, null);
		} else if ("checkExists".equals(opcode)) {
			assertTrue (index.getTree().find(key));
		} else if ("checkNotExists".equals(opcode)) {
			assertFalse (index.getTree().find(key));
		} else {
			printError("Illegal opcode (" + line + "): " + opcode);
			return;
		}
	}
	
	public void printError(String msg) {
		m_stderr.append(msg);
		m_stderr.append("\n");
	}

	protected void loadTest(String file) {
		try {
			m_script = new LineNumberReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		m_stdout = new StringBuilder();
		m_stderr = new StringBuilder();
	}
	
	public void executeTest(String filename) {
		loadTest(filename);

		String line;
		try {
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void testSimpleTest() throws ClassNotFoundException, XmlException, InterruptedException {
		index = new BDBPlusTreeIndex(null, this.m_d, "id", true);
		executeTest("/course/cs127/pub/btree/student-test/simpleTest/clean");
		executeTest("/course/cs127/pub/btree/student-test/simpleTest/insert");
		executeTest("/course/cs127/pub/btree/student-test/simpleTest/delete");
	}
	
	public void testPavlo() throws ClassNotFoundException, XmlException, InterruptedException {
			File dir = new File("/course/cs127/pub/btree/student-test/pavlo");
			for (File child : dir.listFiles()) {
				if (".".equals(child.getName()) || "..".equals(child.getName())) {
					continue;  // Ignore the self and parent aliases.
				}
				index = new BDBPlusTreeIndex(null, this.m_d, "id", true);
				System.out.println(child.getAbsolutePath());
				executeTest(child.getAbsolutePath());
			}        
	}
	
}
