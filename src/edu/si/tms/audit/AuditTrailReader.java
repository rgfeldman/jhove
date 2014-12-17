/**
 CDIS 2.0 - Common Code
 AuditTrailReader.java
 */
package edu.si.tms.audit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.logging.Logger;

public class AuditTrailReader {
	
	Properties auditQueries = null;
	Logger _log = Logger.getLogger(this.getClass().getName());

	public AuditTrailReader(String auditConfigFile) {
		
		try {
			loadProperties(auditConfigFile);
		}
		catch(IOException ioe) {
			_log.info("There was an error loading the AuditConfig file."); 
			ioe.printStackTrace();
		}
		
	}

	private void loadProperties(String auditConfigFile) throws IOException {
			
		//verify file exists
		if(auditConfigFile == null) {
			throw new FileNotFoundException();
		}
		
		auditQueries = new Properties();
		
		//load properties from the file
		File configFile = new File(auditConfigFile);
		FileInputStream inStream = new FileInputStream(configFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
		
		String lineHolder = new String();
		lineHolder = reader.readLine();
		while(lineHolder != null) {
			//_log.info("line: " + lineHolder);
			if(!lineHolder.startsWith("#") && !lineHolder.trim().equals("") && lineHolder.split("=").length > 1) {
				auditQueries.setProperty(lineHolder.split("=")[0].trim(), lineHolder.split("=")[1].trim());
			}
			lineHolder = reader.readLine();
		}
		
		reader.close();
		
		return;
	}
	
	public String getQuery(String tableName) {
		
		if(auditQueries != null) {
			return auditQueries.getProperty(tableName);
		}
		else {
			return null;
		}
		
	}
	
	public ArrayList<?> getTableNames() {
		
		if(auditQueries != null) {
			return new ArrayList<Object>(auditQueries.keySet());
		}
		else {
			return null;
		}
	}

}
