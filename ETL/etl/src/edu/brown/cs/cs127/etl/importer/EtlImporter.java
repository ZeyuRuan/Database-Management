package edu.brown.cs.cs127.etl.importer;

import java.io.FileReader;
import au.com.bytecode.opencsv.CSVReader;

public class EtlImporter
{
	/**
	 * You are only provided with a main method, but you may create as many
	 * new methods, other classes, etc as you want: just be sure that your
	 * application is runnable using the correct shell scripts.
	 */
	public static void main(String[] args) throws Exception
	{
		if (args.length != 4)
		{
			System.err.println("This application requires exactly four parameters: " +
					"the path to the airports CSV, the path to the airlines CSV, " +
					"the path to the flights CSV, and the full path where you would " +
					"like the new SQLite database to be written to.");
			System.exit(1);
		}

		String AIRPORTS_FILE = args[0];
		String AIRLINES_FILE = args[1];
		String FLIGHTS_FILE = args[2];
		String DB_FILE = args[3];
	
		/*
		CSVReader airportsreader = new CSVReader(new FileReader("~/Desktop/etl/airports.csv"));
		String [] nextLine;
		while ((nextLine = airportsreader.readNext()) != null) {
			// nextLine[] is an array of values from the line
			System.out.println(nextLine[0] + nextLine[1] );
		}
		
		CSVReader airlinesreader = new CSVReader(new FileReader("~/Desktop/etl/airports.csv"));
		
		while ((nextLine = airlinesreader.readNext()) != null) {
	 		// nextLine[] is an array of values from the line
	 		System.out.println(nextLine[0] + nextLine[1] + nextLine[2] + nextLine[3] );
	  	}
		*/
		
		// create flight_id: use auto increment
		
		
		/*
		 * READING DATA FROM CSV FILES
		 * Source: http://opencsv.sourceforge.net/#how-to-read
		 * 
		 * If you want to use an Iterator style pattern, you might do something like this: 
		 * 
		 *	CSVReader reader = new CSVReader(new FileReader("yourfile.csv"));
		 *	String [] nextLine;
		 *	while ((nextLine = reader.readNext()) != null) {
		 *		// nextLine[] is an array of values from the line
		 *		System.out.println(nextLine[0] + nextLine[1] + "etc...");
		 * 	}
		 */

		/*
		 * Below are some snippets of JDBC code that may prove useful
		 * 
		 * For more sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 * 
		 * ---
		 * 
		 *	// INITIALIZE THE CONNECTION
		 *	Class.forName("org.sqlite.JDBC");
		 *	Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
		 *
		 * ---
		 *
		 *	// ENABLE FOREIGN KEY CONSTRAINT CHECKING
		 *	Statement stat = conn.createStatement();
		 *	stat.executeUpdate("PRAGMA foreign_keys = ON;");
		 *
		 *	// Speed up INSERTs
		 *	stat.executeUpdate("PRAGMA synchronous = OFF;");
		 *	stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
		 *
		 * ---
		 * 
		 *	// You can execute DELETE statements before importing if you want to be
		 *	// able to overwrite an existing database.
		 *	stat.executeUpdate("DROP TABLE IF EXISTS table;");
		 *
		 * ---
		 *
		 *	// To create a table, you can execute the following command.
		 *	stat.executeUpdate("CREATE TABLE airports (airport_id INTEGER PRIMARY KEY AUTOINCREMENT, airport_code CHAR(3)); ");
		 *
		 * ---
		 * 
		 * 	// Normally the database throws an exception when constraints are enforced
		 *	// and an INSERT statement that violates a constraint is executed. This is true
		 *	// even when doing a batch insert (multiple rows in one statement), causing all
		 *	// rows in the statement to not be inserted into the database.
		 *
		 *	// As a result, if you want the efficiency gains of using batch inserts, you need to be smart:
		 *	// You need to make sure your application enforces foreign key constraints before the insert ever happens.
		 * 	PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO table (col1, col2) VALUES (?, ?)");
		 *  String[] nextLine;
		 *  for ((nextLine = reader.readNext()) != null)
		 *  {
		 *  	prep.setString(1, nextLine[0]);
		 *  	prep.setInt(2, nextLine[1]);
		 *  	prep.addBatch();
		 *  }
		 *  
		 *  // We temporarily disable auto-commit, allowing the batch to be sent
		 *  // as one single transaction. Then we re-enable it, executing the batch.
		 *  conn.setAutoCommit(false);
		 *  prep.executeBatch();
		 *  conn.setAutoCommit(true);
		 * 	
		 */



		/*
		 * Date/Time Normalization Example
		 *
		 * import java.util.Date;
		 * import java.text.DateFormat;
		 * import java.text.ParseException;
		 * import java.text.SimpleDateFormat;
		 *
		 * // a sample date to convert
		 * String sampleDateString = "01-13-1992";
		 *
		 * // choose a standard date format that will be used throughout the database
		 * DateFormat standardFormat = new SimpleDateFormat("yyyy-MM-dd");
		 *
		 * // one possible date format to be converted
		 * DateFormat sampleDateFormat1 = new SimpleDateFormat("MM-dd-yyyy");
		 * // you need to set this flag to false for every possible DateFormat that you will be checking against.
		 * sampleDateFormat1.setLenient(false);
		 *
		 *
		 * try {
		 * 	// if the parse method doesn't throw an exception, then the format matches the date string.
		 *	// in our example, sampleDateFormat1 will match sampleDateString
		 *	Date sampleDate = sampleFormat1.parse(sampleDateString);
		 *	
		 *	// standardize the sample date from "MM-dd-yyyy" to "yyyy-MM-dd"
		 *	String sampleDateStringNormalized = standardFormat.format(sampleDate);
		 *
		 *	// this will print "1992-01-13"
		 *	System.out.println(sampleDateStringNormalized);
		 * }
		 * catch (ParseException e) {
		 *	// the sample format doesn't match the sample date string.
		 *	// now you should try parsing with a different sample format
		 *	return;
		 * }
		 */	

	}
}
