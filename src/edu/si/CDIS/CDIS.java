/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CIS.ImageFilePath;
import java.io.FileInputStream;
import java.util.Properties;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.text.SimpleDateFormat;
import java.util.logging.SimpleFormatter;
import edu.si.CDIS.utilties.DataProvider;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Handler;
import edu.si.CDIS.CIS.TMSIngest;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.DAMS.DAMSIngest;
import java.sql.SQLException;
import java.util.HashMap;
import com.artesia.common.encryption.encryption.EncryptDecrypt;
import org.apache.commons.logging.LogFactory;


public class CDIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    private static FileHandler fh = null;
    
    
    
    public Connection tmsConn;
    public Connection damsConn;
    public String operationType;
    public Properties properties;
    public HashMap <String,String[]> xmlSelectHash;
            
    // handles database connections
    public boolean connectToDatabases (CDIS cdis_new) {
        
        //establish and verify database connections.
	try {
                String tmsPass = EncryptDecrypt.decryptString(cdis_new.properties.getProperty("tmsPass"));
                
		cdis_new.tmsConn = DataProvider.getConnection(cdis_new.properties.getProperty("tmsDriver"), 
                        cdis_new.properties.getProperty("tmsUrl"), 
			cdis_new.properties.getProperty("tmsUser"), 
			tmsPass);
                
                
	} catch(Exception ex) {
		ex.printStackTrace();
		return false;
	}
        
        logger.log(Level.FINER, "Connection to TMS database established.");
        
        try {
            
            String damsPass = EncryptDecrypt.decryptString(cdis_new.properties.getProperty("damsPass"));
            
            cdis_new.damsConn = DataProvider.getConnection(cdis_new.properties.getProperty("damsDriver"), 
					cdis_new.properties.getProperty("damsUrl"), 
					cdis_new.properties.getProperty("damsUser"), 
					damsPass);
                
        } catch(Exception ex) {
		ex.printStackTrace();
		return false;
	}
        
        logger.log(Level.FINER, "Connection to DAMS database established.");
        
        return true;
        
    }
    
    // set the logger parameters
    private boolean setLogger (CDIS cdis_new) {

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
    private boolean readIni (CDIS cdis_new) {
        
        
        
        cdis_new.properties = new Properties();
        
        String iniFile = "conf\\config.ini";
                
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
    public void execute(String operationType) {
       
        
        CDIS cdis_new = new CDIS();
        
        try {
        
            //String operationType = args[0];
            cdis_new.operationType = operationType;
       
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
        
            // Initialize the statistics Report
            StatisticsReport statReport = new StatisticsReport();
            
            // read the XML config file and obtain the selectStatements
            XmlSqlConfig xml = new XmlSqlConfig();             
            boolean xmlReturn = xml.read(cdis_new.operationType, cdis_new.properties.getProperty("xmlSQLFile"));
            if (! xmlReturn) {
                logger.log(Level.SEVERE, "Fatal Error: unable to read/parse sql xml file");
                return;
            }
            
            cdis_new.xmlSelectHash = new HashMap <String, String[]>(xml.getSelectStmtHash());
            
            if (cdis_new.operationType.equals("ingestToCIS")) {          
                TMSIngest tmsIngest = new TMSIngest();
                tmsIngest.ingest(cdis_new, statReport);
            }
            else if (cdis_new.operationType.equals("ingestToDAMS")) {
                DAMSIngest damsIngest = new DAMSIngest();
                damsIngest.ingest(cdis_new, statReport);
            }
            else if (cdis_new.operationType.equals("link")) {
            
                LinkCollections linkcollections = new LinkCollections();
                linkcollections.link(cdis_new, statReport);
            }
            else if (cdis_new.operationType.equals("sync")) {
                 MetaData metaData = new MetaData();
                 metaData.sync(cdis_new, statReport);
        
                 //sync the imageFilePath.  This essentially should be moved out of metadata sync and be called on its own from the main CDIS
                ImageFilePath imgPath = new ImageFilePath();
                imgPath.sync(cdis_new, statReport);
            }
            else if (cdis_new.operationType.equals("thumbnailSync")) {
                 Thumbnail thumbnail = new Thumbnail();
                 thumbnail.sync(cdis_new, statReport);
            }
            else {
                logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting");
                return;
            }
            
            statReport.compile(cdis_new.operationType);
            
            // Send the report by email if there is an email list
            if (cdis_new.properties.getProperty("emailReportTo") != null) {
                logger.log(Level.FINER, "Need to email the report");
                statReport.send(cdis_new.properties.getProperty("siUnit"), cdis_new.properties.getProperty("emailReportTo"), cdis_new.operationType);
            }
        
 
        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try { if ( cdis_new.damsConn != null)  cdis_new.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
            try { if ( cdis_new.tmsConn != null)  cdis_new.tmsConn.close(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( cdis_new.damsConn != null)  cdis_new.damsConn.close(); } catch (Exception e) { e.printStackTrace(); }
        }         
    
    }
    
   
   
  
}
