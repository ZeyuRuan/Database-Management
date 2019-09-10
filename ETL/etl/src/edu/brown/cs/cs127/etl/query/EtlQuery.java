package edu.brown.cs.cs127.etl.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EtlQuery
{
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception
	{
		Class.forName("org.sqlite.JDBC");
		// connect to data.db
		conn = DriverManager.getConnection("jdbc:sqlite:"+pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}

	public ResultSet query1(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out 
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(airport_code) FROM airports"
		);
		//stat.setString(1, args[0]);
		//stat.setInt(2, Integer.parseInt(args[1]));
		return stat.executeQuery();
		
	}
	
	public ResultSet query2(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(airline_code) FROM airlines"
		);
		//stat.setString(1, args[0]);
		//stat.setInt(2, Integer.parseInt(args[1]));
		return stat.executeQuery();
		
	}
	
	public ResultSet query3(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT count(flight_id) FROM flights"
		);
		//stat.setString(1, args[0]);
		//stat.setInt(2, Integer.parseInt(args[1]));
		return stat.executeQuery();
		
	}
	
	public ResultSet query4(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT 'weather delay' as delay_type, count(weather_delay) as frequency from flights where weather_delay>0 union "
			+ "SELECT 'carrier delay' as delay_type, count(carrier_delay) as frequency from flights where carrier_delay>0 union "
			+ "SELECT 'air traffic delay' as delay_type, count(air_traffic_delay) as frequency from flights where air_traffic_delay>0 union "
			+ "SELECT 'security delay' as delay_type, count(security_delay) as frequency from flights where security_delay>0 "
			+ "order by frequency desc"
		);
		//stat.setString(1, args[0]);
		//stat.setInt(2, Integer.parseInt(args[1]));
		return stat.executeQuery();
		
	}
	
	public ResultSet query5(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT origin_airport_code, dest_airport_code, (depart_date||' '||depart_time)as depart_schedule from flights "
			+ "where airline_code=? and flight_num=? and depart_date=?"
		);
		
		stat.setString(1, args[0]);
		stat.setInt(2, Integer.parseInt(args[1]));
		
		String date, month, day, year;
		month = args[2];
		day = args[3];
		year = args[4];
		int mm = Integer.parseInt(args[2]);	if (mm<10) month = '0' + month;
		int dd = Integer.parseInt(args[3]);	if (dd<10) day = '0' + day;		
		date = year + '-' + month + '-' + day;
		stat.setString(3, date);
		
		return stat.executeQuery();
		
	}
	
	public ResultSet query6(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT airline_name,count(flight_id) as num_flights "
			+ "from flights as f join airlines as l on f.airline_code=l.airline_code "
			+ "where depart_date=? group by airline_name "
			+ "order by num_flights desc, airline_name"
		);
		
		String date, month, day, year;
		month = args[0];
		day = args[1];
		year = args[2];
		int mm = Integer.parseInt(args[0]);	if (mm<10) month = '0' + month;
		int dd = Integer.parseInt(args[1]);	if (dd<10) day = '0' + day;		
		date = year + '-' + month + '-' + day;
		stat.setString(1, date);
		
		return stat.executeQuery();
		
	}
	
	public ResultSet query7(String[] args) throws SQLException
	{		
		int num_airportname = args.length-3;
		String airportnames = "'" + args[3] + "'";
		for (int i=1;i<num_airportname;i++){
			airportnames = airportnames + ", '" + args[i+3] + "'";
		}
		/*
		String query = "with dept as (SELECT airport_name, count(flight_id) as num_dept "
				+ "from airports as p join flights as f on p.airport_code=f.origin_airport_code where depart_date= ? group by airport_name), "
				+ "ariv as (SELECT airport_name, count(flight_id) as num_ariv "
				+ "from airports as p join flights as f on p.airport_code=f.dest_airport_code where arrival_date= ? group by airport_name) "
				+ "select * from dept natural join ariv where airport_name in (" + airportnames + ") order by airport_name";
		*/
		
		String query = 
				 "with dept as (SELECT airport_name, count(flight_id) as num_dept "
				 + "from ( select * from airports as p join flights as f on p.airport_code=f.origin_airport_code "
				 + "where depart_date = ? ) group by airport_name) , "
				 + "deptt as (select airport_name, '0' as num_dept from airports where airport_name not in (select airport_name from dept)), "
				 + "dd as (select * from dept union select * from deptt), "
				 + "ariv as (SELECT airport_name, count(flight_id) as num_ariv "
				 + "from  ( select * from airports as p join flights as f on p.airport_code=f.dest_airport_code "
				 + "where arrival_date= ? ) group by airport_name), "
				 + "arivv as (select airport_name, '0' as num_ariv from airports where airport_name not in (select airport_name from ariv)), "
				 + "aa as (select * from ariv union select * from arivv) "
				 + "select * from dd natural join aa where airport_name in (" + airportnames +") order by airport_name";
		
		PreparedStatement stat = conn.prepareStatement(query);
		
		String date, month, day, year;
		month = args[0];	day = args[1];		year = args[2];
		int mm = Integer.parseInt(args[0]);		if (mm<10) month = '0' + month;
		int dd = Integer.parseInt(args[1]);		if (dd<10) day = '0' + day;		
		date = year + '-' + month + '-' + day;
		stat.setString(1, date);
		stat.setString(2, date);
		
		return stat.executeQuery();
		
	}
	
	
	public ResultSet query8(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"SELECT * FROM "
			+ "(SELECT count(flight_id) as num_schedule from flights join airlines "
			+ "where airline_name=? and flight_num=? "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
			+ "join "
			+ "(SELECT count(flight_id) as num_cancelled from flights join airlines "
			+ "where airline_name=? and flight_num=? and cancelled=1 "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
			+ "join "
			+ "(SELECT count(flight_id) as num_dept_early from flights join airlines "
			+ "where airline_name=? and flight_num=? and cancelled=0 and depart_diff<=0 "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
			+ "join "
			+ "(SELECT count(flight_id) as num_dept_late from flights join airlines "
			+ "where airline_name=? and flight_num=?  and cancelled=0 and depart_diff>0 "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
			+ "join "
			+ "(SELECT count(flight_id) as num_ariv_early from flights join airlines "
			+ "where airline_name=? and flight_num=?  and cancelled=0 and arrival_diff<=0 "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
			+ "join "
			+ "(SELECT count(flight_id) as num_aruv_late from flights join airlines "
			+ "where airline_name=? and flight_num=?  and cancelled=0 and arrival_diff>0 "
			+ "and (strftime('%s',depart_date) - strftime('%s',?))>=0 and (strftime('%s',depart_date) - strftime('%s',?))<=0 ) "
		);
		
		String date1, date2;
		String month1, day1, year1, month2, day2, year2;
		String d1 = args[2];
		month1 = d1.substring(0,2);		day1 = d1.substring(3,5);	year1 = d1.substring(6);
		date1 = year1 + '-' + month1 + '-' + day1;
		String d2 = args[3];
		month2 = d2.substring(0,2);		day2 = d2.substring(3,5);	year2 = d2.substring(6);
		date2 = year2 + '-' + month2 + '-' + day2;
		
		stat.setString(1, args[0]);		stat.setString(2, args[1]);		stat.setString(3, date1);		stat.setString(4, date2);
		stat.setString(5, args[0]);		stat.setString(6, args[1]);		stat.setString(7, date1);		stat.setString(8, date2);
		stat.setString(9, args[0]);		stat.setString(10, args[1]);	stat.setString(11, date1);		stat.setString(12, date2);
		stat.setString(13, args[0]);	stat.setString(14, args[1]);	stat.setString(15, date1);		stat.setString(16, date2);
		stat.setString(17, args[0]);	stat.setString(18, args[1]);	stat.setString(19, date1);		stat.setString(20, date2);
		stat.setString(21, args[0]);	stat.setString(22, args[1]);	stat.setString(23, date1);		stat.setString(24, date2);
		
		return stat.executeQuery();
		
	}
	
	public ResultSet query9(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"select *, (strftime('%s',real_ariv)-strftime('%s',real_dept))/60 as duration from "
			+ "(select airline_code, flight_num, "
			+ "origin_airport_code, strftime('%H:%M', time(depart_time, depart_diff||' minutes')) as real_dept, "
			+ "dest_airport_code, strftime('%H:%M', time(arrival_time, arrival_diff||' minutes')) as real_ariv "
			+ "from flights where flight_id in( "
			+ "select flight_id from flights as f join airlines join airports as p on f.origin_airport_code=p.airport_code "
			+ "where city= ? and state= ? and depart_date = ? "
			+ "intersect "
			+ "select flight_id from flights as f join airlines join airports as p on f.dest_airport_code=p.airport_code "
			+ "where city= ? and state= ? and arrival_date= ? ) ) "
			);
		
		String date, month, day, year;
		String d = args[4];			month = d.substring(0,2);		day = d.substring(3,5);		year = d.substring(6);
		date = year + '-' + month + '-' + day;
		
		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, date);
		stat.setString(4, args[2]);
		stat.setString(5, args[3]);
		stat.setString(6, date);
		
		return stat.executeQuery();
		
	}
	
	public ResultSet query10(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"with h1 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date1, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date1 "
			+ "from airports join flights on airports.airport_code=flights.origin_airport_code "
			 + "where city = ? and state = ? and real_dept_date1 = ? and real_ariv_date1 = ?),"
			 + "hop1 as (select * from h1 join airports on airports.airport_code=h1.dest_airport_code "
			 + "where h1.city <> airports.city),"
			 + "h2 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date2, "
			 + "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date2 "
			 + "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			 + "where city = ? and state = ? and real_dept_date2 = ? and real_ariv_date2 = ? ),"
			 + "hop2 as (select * from h2 join airports on airports.airport_code=h2.origin_airport_code "
			 + "where h2.city <> airports.city)"
			 
			 + "select *, (strftime('%s',real_ariv2)-strftime('%s',real_dept1))/60 as duration from ("
			 + "select "
			 + "hop1.airline_code as l1, hop1.flight_num, "
			 + "hop1.origin_airport_code, "
			 + "strftime('%H:%M', time(hop1.depart_time, hop1.depart_diff||' minutes')) as real_dept1, "
			 + "hop1.dest_airport_code, "
			 + "strftime('%H:%M', time(hop1.arrival_time, hop1.arrival_diff||' minutes')) as real_ariv1, "
			 + "hop2.airline_code as l2, hop2.flight_num, "
			 + "hop2.origin_airport_code, "
			 + "strftime('%H:%M', time(hop2.depart_time, hop2.depart_diff||' minutes')) as real_dept2, "
			 + "hop2.dest_airport_code, "
			 + "strftime('%H:%M', time(hop2.arrival_time, hop2.arrival_diff||' minutes')) as real_ariv2 "
			 + "from hop1 join hop2 on hop1.dest_airport_code=hop2.origin_airport_code "
			 + "where strftime('%s',real_ariv1)-strftime('%s',real_dept2) < 0 )  "
			 + "order by duration, l1, l2"
			);
		
		String date, month, day, year;
		String d = args[4];			month = d.substring(0,2);		day = d.substring(3,5);		year = d.substring(6);
		date = year + '-' + month + '-' + day;
		
		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, date);
		stat.setString(4, date);
		stat.setString(5, args[2]);
		stat.setString(6, args[3]);
		stat.setString(7, date);
		stat.setString(8, date);
		
		return stat.executeQuery();
		
	}
	
	public ResultSet query11(String[] args) throws SQLException
	{
		
		PreparedStatement stat = conn.prepareStatement(
			"with h1 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date1, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date1 "
			+ "from airports join flights on airports.airport_code=flights.origin_airport_code "
			+ "where city = ? and state = ? and real_dept_date1 = ? and real_ariv_date1 = ?), "
			+ "hop1 as (select * from h1 join airports on airports.airport_code=h1.dest_airport_code "
			+ "where h1.city <> airports.city), "
			+ ""
			+ "h2 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date2, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date2 "
			+ "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			+ "where real_dept_date2 = ? and real_ariv_date2 = ? ), "
			+ "hop2 as (select * from h2 join airports on airports.airport_code=h2.origin_airport_code "
			+ "where h2.city <> airports.city), "
			+ ""
			+ "h3 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date3, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date3 "
			+ "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			+ "where city = ? and state = ? and real_dept_date3 = ? and real_ariv_date3 = ? ), "
			+ "hop3 as (select * from h3 join airports on airports.airport_code=h3.origin_airport_code "
			+ "where h3.city <> airports.city) "
			+ ""
			+ "select *, (strftime('%s',real_ariv3)-strftime('%s',real_dept1))/60 as duration from ( "
			+ "select hop1.airline_code as l1, hop1.flight_num, "
			+ "hop1.origin_airport_code, "
			+ "strftime('%H:%M', time(hop1.depart_time, hop1.depart_diff||' minutes')) as real_dept1, "
			+ "hop1.dest_airport_code,  "
			+ "strftime('%H:%M', time(hop1.arrival_time, hop1.arrival_diff||' minutes')) as real_ariv1, "
			+ ""
			+ "hop2.airline_code as l2, hop2.flight_num, "
			+ "hop2.origin_airport_code, "
			+ "strftime('%H:%M', time(hop2.depart_time, hop2.depart_diff||' minutes')) as real_dept2, "
			+ "hop2.dest_airport_code, "
			+ "strftime('%H:%M', time(hop2.arrival_time, hop2.arrival_diff||' minutes')) as real_ariv2, "
			+ ""
			+ "hop3.airline_code as l3, hop3.flight_num, "
			+ "hop3.origin_airport_code, "
			+ "strftime('%H:%M', time(hop3.depart_time, hop3.depart_diff||' minutes')) as real_dept3, "
			+ "hop3.dest_airport_code, "
			+ "strftime('%H:%M', time(hop3.arrival_time, hop3.arrival_diff||' minutes')) as real_ariv3  "
			+ ""
			+ "from hop1 join hop2 on hop1.dest_airport_code=hop2.origin_airport_code "
			+ "join hop3 on hop2.dest_airport_code=hop3.origin_airport_code "
			+ "where strftime('%s',real_ariv1)-strftime('%s',real_dept2) < 0 "
			+ "and strftime('%s',real_ariv2)-strftime('%s',real_dept3) < 0  )"
			+ "order by duration, l1, l2, l3 limit 30"
			);
		
		String date, month, day, year;
		String d = args[4];			month = d.substring(0,2);		day = d.substring(3,5);		year = d.substring(6);
		date = year + '-' + month + '-' + day;
		
		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, date);
		stat.setString(4, date);
		stat.setString(5, date);
		stat.setString(6, date);
		stat.setString(7, args[2]);
		stat.setString(8, args[3]);
		stat.setString(9, date);
		stat.setString(10, date);
		
		return stat.executeQuery();
		
	}
	
	
	
	
	
	/*
	1. SELECT count(airport_code) FROM airports
	
	2. SELECT count(airline_code) FROM airlines
	
	3. SELECT count(flight_id) FROM flights
	
	4. SELECT 'weather delay' as delay_type, count(weather_delay) as frequency from flights having weather_delay>0 union 
	SELECT 'carrier delay' as delay_type, count(carrier_delay) as frequency from flights having carrier_delay>0 union 
	SELECT 'air traffic delay' as delay_type, count(air_traffic_delay) as frequency from flights having air_traffic_delay>0 union 
	SELECT 'security delay' as delay_type, count(security_delay) as frequency from flights having security_delay>0 
	order by frequency desc"
	use SetString to change inputs into the standard form yyyy-mm-dd
	
	5. SELECT origin_airport_code, dest_airport_code, (depart_date||' '||depart_time)as depart_schedule from flights 
	where airline_code='AS' and flight_num='692' and depart_date='yyyy-mm-dd'
	
	6. SELECT airline_name,count(flight_id) as num_flights 
	from flights as f join airlines as l on f.airline_code=l.airline_code
	where depart_date='yyyy-mm-dd'
	group by airline_name order by num_flights desc, airline_name
	
	7.with 	dept as (SELECT airport_name, count(flight_id) as num_dept 
				from airports as p join flights as f on p.airport_code=f.origin_airport_code where depart_date='2012-01-24' group by airport_name),
     		ariv as (SELECT airport_name, count(flight_id) as num_ariv 
     			from airports as p join flights as f on p.airport_code=f.dest_airport_code where arrival_date='2012-01-24' group by airport_name)
	select * from dept natural join ariv where airport_name='Tampa International' order by airport_name
	
	8.
	SELECT * FROM 
	(SELECT count(flight_id) as num_schedule from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692) join
	(SELECT sum(cancelled) as num_cancelled from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692) join
	(SELECT count(flight_id) as num_dept_early from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692 and cancelled=0 and depart_diff<=0) join
	(SELECT count(flight_id) as num_dept_late from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692 and cancelled=0 and depart_diff>0 ) join
	(SELECT count(flight_id) as num_ariv_early from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692 and cancelled=0 and arrival_diff<=0 ) join
	(SELECT count(flight_id) as num_aruv_late from flights as f join airlines as l on f.airline_code=l.airline_code where f.airline_code='AS' and flight_num=692 and cancelled=0 and arrival_diff>0 )
	
	9.
	select *, (strftime('%s',real_ariv)-strftime('%s',real_dept))/60 as duration from 
	  (select airline_code, flight_num, 
 		origin_airport_code, strftime("%H:%M", time(depart_time, depart_diff||' minutes')) as real_dept, 
 		dest_airport_code, strftime("%H:%M", time(arrival_time, arrival_diff||' minutes')) as real_ariv 
		from flights where flight_id in(
  			select flight_id from flights as f join airlines join airports as p on f.origin_airport_code=p.airport_code 
  				where city='New York' and state='New York' and depart_date='2012-01-30' 
  			intersect
  			select flight_id from flights as f join airlines join airports as p on f.dest_airport_code=p.airport_code 
  				where city='Chicago' and state='Illinois' and arrival_date='2012-01-30' ) )
	10.
	  "with h1 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date1, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date1 "
			+ "from airports join flights on airports.airport_code=flights.origin_airport_code "
			 + "where city = ? and state = ? and real_dept_date1 = ? and real_ariv_date1 = ?),"
			 + "hop1 as (select * from h1 join airports on airports.airport_code=h1.dest_airport_code "
			 + "where h1.city <> airports.city),"
			 + "h2 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date2, "
			 + "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date2 "
			 + "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			 + "where city = ? and state = ? and real_dept_date2 = ? and real_ariv_date2 = ? ),"
			 + "hop2 as (select * from h2 join airports on airports.airport_code=h2.origin_airport_code "
			 + "where h2.city <> airports.city)"
			 
			 + "select *, (strftime('%s',real_ariv2)-strftime('%s',real_dept1))/60 as duration from ("
			 + "select "
			 + "hop1.airline_code as l1, hop1.flight_num, "
			 + "hop1.origin_airport_code, "
			 + "strftime('%H:%M', time(hop1.depart_time, hop1.depart_diff||' minutes')) as real_dept1, "
			 + "hop1.dest_airport_code, "
			 + "strftime('%H:%M', time(hop1.arrival_time, hop1.arrival_diff||' minutes')) as real_ariv1, "
			 + "hop2.airline_code as l2, hop2.flight_num, "
			 + "hop2.origin_airport_code, "
			 + "strftime('%H:%M', time(hop2.depart_time, hop2.depart_diff||' minutes')) as real_dept2, "
			 + "hop2.dest_airport_code, "
			 + "strftime('%H:%M', time(hop2.arrival_time, hop2.arrival_diff||' minutes')) as real_ariv2 "
			 + "from hop1 join hop2 on hop1.dest_airport_code=hop2.origin_airport_code "
			 + "where strftime('%s',real_ariv1)-strftime('%s',real_dept2) < 0 )  "
			 + "order by duration, l1, l2"
	
	11.
	
	"with h1 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date1, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date1 "
			+ "from airports join flights on airports.airport_code=flights.origin_airport_code "
			+ "where city = ? and state = ? and real_dept_date1 = ? and real_ariv_date1 = ?), "
			+ "hop1 as (select * from h1 join airports on airports.airport_code=h1.dest_airport_code "
			+ "where h1.city <> airports.city), "
			+ ""
			+ "h2 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date2, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date2 "
			+ "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			+ "where real_dept_date2 = ? and real_ariv_date2 = ? ), "
			+ "hop2 as (select * from h2 join airports on airports.airport_code=h2.origin_airport_code "
			+ "where h2.city <> airports.city), "
			+ ""
			+ "h3 as ( select *, strftime('%Y-%m-%d', datetime(depart_date || depart_time, depart_diff||' minutes')) as real_dept_date3, "
			+ "strftime('%Y-%m-%d', datetime(arrival_date || arrival_time, arrival_diff||' minutes')) as real_ariv_date3 "
			+ "from airports join flights on airports.airport_code=flights.dest_airport_code  "
			+ "where city = ? and state = ? and real_dept_date3 = ? and real_ariv_date3 = ? ), "
			+ "hop3 as (select * from h3 join airports on airports.airport_code=h3.origin_airport_code "
			+ "where h3.city <> airports.city) "
			+ ""
			+ "select *, (strftime('%s',real_ariv3)-strftime('%s',real_dept1))/60 as duration from ( "
			+ "select hop1.airline_code as l1, hop1.flight_num, "
			+ "hop1.origin_airport_code, "
			+ "strftime('%H:%M', time(hop1.depart_time, hop1.depart_diff||' minutes')) as real_dept1, "
			+ "hop1.dest_airport_code,  "
			+ "strftime('%H:%M', time(hop1.arrival_time, hop1.arrival_diff||' minutes')) as real_ariv1, "
			+ ""
			+ "hop2.airline_code as l2, hop2.flight_num, "
			+ "hop2.origin_airport_code, "
			+ "strftime('%H:%M', time(hop2.depart_time, hop2.depart_diff||' minutes')) as real_dept2, "
			+ "hop2.dest_airport_code, "
			+ "strftime('%H:%M', time(hop2.arrival_time, hop2.arrival_diff||' minutes')) as real_ariv2, "
			+ ""
			+ "hop3.airline_code as l3, hop3.flight_num, "
			+ "hop3.origin_airport_code, "
			+ "strftime('%H:%M', time(hop3.depart_time, hop3.depart_diff||' minutes')) as real_dept3, "
			+ "hop3.dest_airport_code, "
			+ "strftime('%H:%M', time(hop3.arrival_time, hop3.arrival_diff||' minutes')) as real_ariv3  "
			+ ""
			+ "from hop1 join hop2 on hop1.dest_airport_code=hop2.origin_airport_code "
			+ "join hop3 on hop2.dest_airport_code=hop3.origin_airport_code "
			+ "where strftime('%s',real_ariv1)-strftime('%s',real_dept2) < 0 "
			+ "and strftime('%s',real_ariv2)-strftime('%s',real_dept3) < 0  )"
			+ "order by duration, l1, l2, l3 limit 30"
	
	
	
*/
}
