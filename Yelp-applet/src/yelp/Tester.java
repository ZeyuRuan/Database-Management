package yelp;

import java.io.*;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

/**
 * This tester is for the grading purpose. 
 * Students will not have permissions to access output files so running these JUnits won't do anything.
 *
 */
public class Tester extends TestCase
{
	static String TEST_PATH = "/course/cs127/pub/yelp/tests/";
	DBController controller;
	
	protected void setUp() throws Exception
	{
		controller = new DBStudentController();
	}

	public void testQuery1() throws SQLException
	{
		String QUERY1_OUTPUT = TEST_PATH + "query1/output";
		checkBusinessObjectList(controller.query1(), QUERY1_OUTPUT);
	}
	
	public void testQuery2() throws SQLException
	{
		String QUERY2_PATH = TEST_PATH + "query2/";
		File[] files = new File(QUERY2_PATH).listFiles();
		Arrays.sort(files);
		for (File file : files)
		{
			String[] spt = file.getAbsolutePath().split("/");
			String businessId = spt[spt.length-1];
			checkReviewObjectList(controller.query2(businessId), file.getAbsolutePath());
		}
	}
	
	public void testQuery3() throws SQLException
	{
		String QUERY3_PATH = TEST_PATH + "query3/";
		File[] files = new File(QUERY3_PATH).listFiles();
		Arrays.sort(files);
		for (File file : files)
		{
			String[] spt = file.getAbsolutePath().split("/");
			String userId = spt[spt.length-1];
			checkAvg(controller.query3(userId), file.getAbsolutePath());
		}
	}	
	public void testQuery4() throws SQLException
	{
		String QUERY1_OUTPUT = TEST_PATH + "query4/output";
		checkBusinessObjectList(controller.query4(), QUERY1_OUTPUT);
	}
	
	public void testQuery5() throws SQLException
	{
		String QUERY1_OUTPUT = TEST_PATH + "query5/output";
		checkBusinessObjectList(controller.query5(), QUERY1_OUTPUT);
	}
	
	protected void checkBusinessObjectList(List<BusinessObject> businessObjectList, String outputPath)
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(outputPath), "UTF8"));
			String line;
			int lineCount = 0;
			while ((line = br.readLine())!= null) lineCount++;
			br.close();
			
			lineCount = 0;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(outputPath), "UTF8"));
			while ((line = br.readLine())!= null)
			{
				String test_line = getBusinessObjectString(businessObjectList.get(lineCount));
				if (!test_line.equals(line))
				{
					// check all fields except percentage. lazy check
					String[] line_spt = line.split(",");
					String[] test_line_spt = test_line.split(",");
					assertEquals(line_spt.length, test_line_spt.length);
					for (int i=0; i<line_spt.length; i++)
					{
						if (i != (line_spt.length-3))
						{
							assertEquals(line_spt[i], test_line_spt[i]);
						}
						else System.out.println(line_spt[i]);
					}
				}
				lineCount++;
			}
			assertEquals(lineCount, businessObjectList.size());
				
			br.close();
		}
		catch(FileNotFoundException e)
		{
			fail("output file not found!");
		}
		catch(Exception e) 
		{ 
			e.printStackTrace(); 
			fail();
		}
	}
	
	protected void checkReviewObjectList(List<ReviewObject> reviewObjectList, String outputPath)
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(outputPath), "UTF8"));
			String line;
			int lineCount = 0;
			while ((line = br.readLine())!= null) lineCount++;
			br.close();
			
			lineCount = 0;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(outputPath), "UTF8"));
			while ((line = br.readLine())!= null)
			{
				assertEquals(line, getReviewObjectString(reviewObjectList.get(lineCount)));
				lineCount++;
			}
			assertEquals(lineCount, reviewObjectList.size());
			br.close();
		}
		catch(FileNotFoundException e)
		{
			fail("output file not found!");
		}
		catch(Exception e) 
		{ 
			e.printStackTrace(); 
			fail();
		}
	}
	
	protected void checkAvg(double average, String outputPath)
	{
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(outputPath), "UTF8"));
			String line = br.readLine();
			assertEquals(Double.parseDouble(line), average);
			br.close();
		}
		catch(FileNotFoundException e)
		{
			fail("output file not found!");
		}
		catch(Exception e) 
		{ 
			e.printStackTrace(); 
			fail();
		}
	}
	
	protected static String getBusinessObjectString(BusinessObject business)
	{
		return String.format("%s,%s,%s,%s,%f,%f,%d,%d",
				business.getId(), 
				business.getName(), 
				business.getAddress().replace('\n', '\t'), 
				business.getPhotoUrl(), 
				business.getStars(), 
				business.getPercentage(),
				business.getReviewCount(),
				business.getEliteCount()
				);
	}
	
	protected static String getReviewObjectString(ReviewObject review)
	{
		return String.format("%s,%s,%s,%f",
				review.getId(), 
				review.getName(), 
				review.getText().replace('\n', '\t'),
				review.getStars()
				);
	}
	
	public static void printBusinessObjectList(List<BusinessObject> businessObjectList, String queryType)
	{
		String filePath = TEST_PATH + queryType + "/output";
		try
		{
			BufferedWriter bw = new BufferedWriter
				    (new OutputStreamWriter(new FileOutputStream(filePath),"UTF-8"));
			for (BusinessObject business : businessObjectList)
			{
				String str = getBusinessObjectString(business);
				bw.write(str);
				bw.newLine();
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	public static void printReviewObjectList(List<ReviewObject> reviewObjectList, String businessId)
	{
		String filePath = TEST_PATH + "query2/"+businessId;
		try
		{
			BufferedWriter bw = new BufferedWriter
				    (new OutputStreamWriter(new FileOutputStream(filePath),"UTF-8"));
			for (ReviewObject review : reviewObjectList)
			{
				String str = getReviewObjectString(review);
				bw.write(str);
				bw.newLine();
			}
			bw.flush();
			bw.close();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
	
	public static void printAvg(double average, String userId)
	{
		String filePath = TEST_PATH + "query3/"+userId;
		try
		{
			BufferedWriter bw = new BufferedWriter
				    (new OutputStreamWriter(new FileOutputStream(filePath),"UTF-8"));
			bw.write(Double.toString(average));
			bw.flush();
			bw.close();
		}
		catch(Exception e) { e.printStackTrace(); }
	}
}
