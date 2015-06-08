/**
 CDIS 2.0 - Common Code
 DataProvider.java
 */
 package edu.si.CDIS.utilties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DataProvider {
	
	
	public static Connection getConnection(String driver, String url, String user, String pass) throws Exception {
		
		Connection conn = null;

	    try
	    {
	      Class.forName(driver);
	    }
	    catch (Exception e)
	    {
	      throw new Exception(
	          "Class.forName failed.\n"
	          +"Exception: " + e.getMessage() + "\n"
	          +"Driver: " + driver + "\n");
	    }

	    try
	    {
	    	conn = DriverManager.getConnection(url, user, pass);
	    }
	    catch (Exception e)
	    {
	    	throw new Exception(
	    			"DriverManager.getConnection failed.\n"
						+"Exception: " + e.getMessage() + "\n"
						+"Properties:\n"
						+"url=" + url + "\n"
						+"user=" + user + "\n");
	    	
	    }

	    return conn;
		
	}
	
	public static ResultSet executeSelect(Connection conn, String sql) {
		
		PreparedStatement stmt = null;
		ResultSet rs = null;
		
		try {
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			return null;
		}
		
		
		return rs;
	}
	
	public static ResultSet executeSelect(Connection conn, PreparedStatement statement) {
		
		PreparedStatement stmt = statement;
		ResultSet rs = null;
		
		try {
			rs = stmt.executeQuery();
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			return null;
		}
                		
		return rs;
	}
	
	public static boolean executeInsert(Connection conn, String sql) {
		
		Statement stmt = null;
		int rowCount;
		boolean success = true;
		
		try {
			stmt = conn.createStatement();
			rowCount = stmt.executeUpdate(sql);
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			return false;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
		}
		
		
		return success;
		
	}
	
	public static int executeUpdate(Connection conn, String sql) {
		
		Statement stmt = null;
		int rowCount;
		
		try {
			stmt = conn.createStatement();
			rowCount = stmt.executeUpdate(sql);
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			return 0;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
		}
		
		
		return rowCount;
		
	}
	
	public static int executeUpdate(Connection conn, PreparedStatement statement) {
		
		PreparedStatement stmt = statement;
		int rowCount;
		
		try {
			rowCount = stmt.executeUpdate();
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
			return 0;
		}
		finally {
			try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
		}
		
		return rowCount;
		
	}

}
