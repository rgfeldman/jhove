/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned;

import java.io.FileInputStream;
import java.util.Properties;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.text.SimpleDateFormat;
import java.util.logging.SimpleFormatter;
import edu.si.data.DataProvider;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Handler;


public class CDIS_new {
    
    private final static Logger logger = Logger.getLogger(CDIS_new.class.getName());
    private static FileHandler fh = null;
    
    
    
    Connection tmsConn;
    Connection damsConn;
    String operationType;
    Properties properties;

            
    // handles database connections
    public boolean connectToDatabases (CDIS_new cdis_new) {
        
        //establish and verify database connections.
	try {
		cdis_new.tmsConn = DataProvider.getConnection(cdis_new.properties.getProperty("tmsDriver"), 
                        cdis_new.properties.getProperty("tmsUrl"), 
			cdis_new.properties.getProperty("tmsUser"), 
			cdis_new.properties.getProperty("tmsPass"));
                
                
	} catch(Exception ex) {
		ex.printStackTrace();
		return false;
	}
        
        logger.log(Level.FINER, "Connection to TMS database established.");
        
        try {
		cdis_new.damsConn = DataProvider.getConnection(cdis_new.properties.getProperty("damsDriver"), 
					cdis_new.properties.getProperty("damsUrl"), 
					cdis_new.properties.getProperty("damsUser"), 
					cdis_new.properties.getProperty("damsPass"));
                
        } catch(Exception ex) {
		ex.printStackTrace();
		return false;
	}
        
        logger.log(Level.FINER, "Connection to DAMS database established.");
        
        return true;
        
    }
    
    // set the logger parameters
    private boolean setLogger (CDIS_new cdis_new) {

        //log All events
        logger.setLevel(Level.ALL);
	
        Handler fh = null;
	DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmm");
	
        try {
		fh = new FileHandler("log\\CDISLog-" + cdis_new.operationType + df.format(new Date()) + ".txt");
	
        } catch (Exception e) {
		e.printStackTrace();
                return false;
	}
        
        
        fh.setFormatter(new SimpleFormatter());          
	logger.addHandler(fh);
	
        logger.log(Level.FINER, "Logging Established");
        fh.flush();
        
        return true;
        
    }
    
    //asign the properties from the ini file to the properties object
    private boolean readIni (CDIS_new cdis_new) {
        
        
        
        cdis_new.properties = new Properties();
        
        String iniFile = "conf\\" + cdis_new.operationType + ".ini";
                
        try {
            logger.log(Level.FINER, "Loading ini file: " + iniFile);
            
            cdis_new.properties.load(new FileInputStream(iniFile));
            
            logger.log(Level.FINER, "Ini File loaded");
            
            
            
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        
        return true;
        
    }
    
    // the main execute function...this later will be changed to the main routine 
    //public static void main(String[] args) {
    public void execute(String executionType) {
       
        CDIS_new cdis_new = new CDIS_new();
        
        //String operationType = args[0];
        cdis_new.operationType = executionType;
       
        cdis_new.properties = new Properties();
        
        //set the logger
        boolean loggingSet = setLogger(cdis_new);
        if (! loggingSet) {
            System.out.println("Fatal Error: Failure to set Logger, exiting");
            return;
        }
        
        //handle the ini file
        boolean iniRead = readIni (cdis_new);
        if (! iniRead) {
            logger.log(Level.SEVERE, "Fatal Error: Failure to Load ini file, exiting");
            return;
        }
        
       
        boolean databaseConnected = connectToDatabases(cdis_new);
        if (! databaseConnected) {
            logger.log(Level.SEVERE, "Fatal Error: Failure to Connect to database");
            return;
        }
        
        LinkCollections linkcollections = new LinkCollections();
        
        if (cdis_new.operationType.equals("link")) {
                    
            linkcollections.link(cdis_new);
        }
 
        try {
            cdis_new.tmsConn.close();
            cdis_new.tmsConn.close(); 
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
                
    
    }
    
   
   
  
}
