/**
 CDIS 2.0 - Common Code
 TMSMediaRenditionFactory.java
 */
package edu.si.tms.factory;

import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Logger;

import edu.si.tms.TMSMediaRendition;

public class TMSMediaRenditionFactory {
	
	public TMSMediaRendition retrieveRendition(String renditionID, Properties properties, Connection conn, Logger log) {
		
		TMSMediaRendition rendition;
		boolean success = false;
		
		try {
			Class<?> dataClass = Class.forName(properties.getProperty("dataClassName"));
			
			rendition = (TMSMediaRendition)dataClass.newInstance();
			
			rendition.setRepositoryCode(properties.getProperty("repositoryCode"));
			success = rendition.populate(conn, renditionID, log);
			
		}
		catch(ClassNotFoundException cnfe) {
			cnfe.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		if(success) {
			return rendition;
		}
		else {
			return null;
		}
		
	}

}
