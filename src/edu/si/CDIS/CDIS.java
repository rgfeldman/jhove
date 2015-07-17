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
import java.text.DateFormat;
import java.util.Date;
import java.util.logging.Handler;
import edu.si.CDIS.CIS.TMSIngest;
import edu.si.CDIS.CIS.Thumbnail;
import edu.si.CDIS.DAMS.DAMSIngest;
import java.util.HashMap;
import com.artesia.common.encryption.encryption.EncryptDecrypt;
import java.io.File;


public class CDIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    public Connection cisConn;
    public Connection damsConn;
    public String operationType;
    public Properties properties;
    public HashMap <String,String[]> xmlSelectHash;
    Long batchNumber;
            
    public Long getBatchNumber () {
        return this.batchNumber;
    }
    
    private void setBatchNumber (Long batchNumber) {
        this.batchNumber = batchNumber;
    }
    
    /*  Method :        connectToDatabases
        Arguments:      
        Description:    establishes connections to both DAMS and CIS database s
        RFeldman 2/2015
    */
    public boolean connectToDatabases () {
        
        //establish and verify database connections.
	
        try {
            //decrypt the password, it is stored in ini in encypted fashion
            String damsPass = EncryptDecrypt.decryptString(this.properties.getProperty("damsPass"));
            
            this.damsConn = DataProvider.getConnection(this.properties.getProperty("damsDriver"), 
					this.properties.getProperty("damsConnString"), 
					this.properties.getProperty("damsUser"), 
					damsPass);
                
        } catch(Exception ex) {
		logger.log(Level.SEVERE, "Failure Connecting to DAMS database.", ex);
		return false;
	}
        
        logger.log(Level.FINER, "Connection to DAMS database established.");
        
        
        try {
                if (this.properties.getProperty("cisSourceDB").equals("CDISDB")) {
                    
                    logger.log(Level.FINER, "CIS source is CDISDB. No connection to CIS database needed.");
                    
                } else {
                    //decrypt the password, it is stored in ini in encypted fashion
                    String tmsPass = EncryptDecrypt.decryptString(this.properties.getProperty("cisPass"));
                
                    this.cisConn = DataProvider.getConnection(this.properties.getProperty("cisDriver"), 
                    this.properties.getProperty("cisConnString"), 
                    this.properties.getProperty("cisUser"), 
                    tmsPass);
                    
                    logger.log(Level.FINER, "Connection to CIS database established.");
                }
                
	} catch(Exception ex) {
		logger.log(Level.SEVERE, "Failure Connecting to CIS database.", ex);
		return false;
        } 
        
        return true;
        
    }
    
    /*  Method :        setLogger
        Arguments:      
        Description:    establishes logger settings
        RFeldman 2/2015
    */
    private boolean setLogger () {

        //log All events
        logger.setLevel(Level.ALL);
	
        Handler fh = null;
	DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmm");
	
        try {
		fh = new FileHandler("log\\CDISLog-" + this.operationType + df.format(new Date()) + ".txt");
	
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
    
    /*  Method :        setLogger
        Arguments:      
        Description:    assigns values from the .ini file into the properties object
        RFeldman 2/2015
    */
    private boolean readIni () {
       
        this.properties = new Properties();
        
        String iniFile = "conf\\config.ini";
                
        try {
            logger.log(Level.FINER, "Loading ini file: " + iniFile);
            
            this.properties.load(new FileInputStream(iniFile));
            
            logger.log(Level.FINER, "Ini File loaded");
            
            
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: obtaining config information", e );
            return false;
        }
        
        return true;
        
    }
    
    private boolean calculateBatchNumber () {    
        try {
        
            DateFormat df = new SimpleDateFormat("yyyyMMddkkmmss");
            setBatchNumber (Long.parseLong(df.format(new Date())));
            
        } catch (Exception e) {
             logger.log(Level.FINER, "Error: obtaining Batch number", e );
             return false;
        }
        
        return true;
    }
    
    
    private boolean verifyProps (Properties properties) {
        
        //verify global properties exist in config file
            String[] requiredProps = {"siUnit",
                                    "damsDriver",
                                    "damsConnString",
                                    "damsUser",
                                    "damsPass",
                                    "cisSourceDB",
                                    "xmlSQLFile"};
        
        for(int i = 0; i < requiredProps.length; i++) {
            String reqProp = requiredProps[i];
            if(!properties.containsKey(reqProp)) {
                logger.log(Level.SEVERE, "Missing required property: {0}", reqProp);
                return false;
            }
        }
        return true;
    }    
    
    /*  Method :        deleteLogs
        Arguments:      
        Description:    deletes old logfiles...or any other file types based on the date
        RFeldman 2/2015
    */
    public void deleteLogs (String folder, String fileNamePrefix, int numDays) {	
            
        File folderDir = new File(folder);
        File[] logs = folderDir.listFiles();
	
        if (logs != null) {
            for(int i = 0; i < logs.length; i++) {
                File tempFile = logs[i];
                if(tempFile.getName().startsWith(fileNamePrefix)) {
                    
                    long diff = new Date().getTime() - tempFile.lastModified();
                    if (diff > numDays * 24 * 60 * 60 * 1000) {
                        tempFile.delete();
                    }
                }				
            }
        }
        
    }
    
    /*  Method :        main
        Arguments:      
        Description:    starting point of the CDIS application
        RFeldman 2/2015
    */
    public static void main(String[] args) {
       
        CDIS cdis = new CDIS();
        
        // Delete old tmp, log and report files
        cdis.deleteLogs("tmp","success",3);
        cdis.deleteLogs("tmp","fail",3);
        cdis.deleteLogs("tmp","header",3);
        cdis.deleteLogs("rpt","Rpt_",14);
        cdis.deleteLogs("log","CDISLog-",14);
        
        // Check if the required number of arguments are inputted
        if(args.length < 1) {
            System.out.println("Missing parameter: <operationType>");
            return;
	}
	else {
            cdis.operationType = args[0];
	}

        try {
        
            cdis.properties = new Properties();
              
            //set the logger
            boolean loggingSet = cdis.setLogger();
            if (! loggingSet) {
                System.out.println("Fatal Error: Failure to set Logger, exiting");
                return;
            }
        
            //handle the ini file
            boolean iniRead = cdis.readIni ();
            if (! iniRead) {
                logger.log(Level.SEVERE, "Fatal Error: Failure to Load ini file, exiting");
                return;
            }
            
            boolean propsVerified = cdis.verifyProps (cdis.properties);
            if (! propsVerified) {
                logger.log(Level.SEVERE, "Fatal Error: Required Property missing.  Exiting");
                return;
            }
        
            boolean databaseConnected = cdis.connectToDatabases();
            if (! databaseConnected) {
                logger.log(Level.SEVERE, "Fatal Error: Failure to Connect to database");
                return;
            }
            
            boolean batchNumberSet = cdis.calculateBatchNumber();
            if (! batchNumberSet) {
                logger.log(Level.SEVERE, "Fatal Error: Batch number could not be generated");
                return;
            }
                    
            // Initialize the statistics Report
            StatisticsReport statReport = new StatisticsReport();
            
            // read the XML config file and obtain the selectStatements
            XmlSqlConfig xml = new XmlSqlConfig();             
            boolean xmlReturn = xml.read(cdis.operationType, cdis.properties.getProperty("xmlSQLFile"));
            if (! xmlReturn) {
                logger.log(Level.SEVERE, "Fatal Error: unable to read/parse sql xml file");
                return;
            }
            
            cdis.xmlSelectHash = new HashMap <String, String[]>(xml.getSelectStmtHash());
            
            switch (cdis.operationType) {
                case "createCISmedia" :
                    TMSIngest tmsIngest = new TMSIngest();
                    tmsIngest.ingest(cdis, statReport);
                    break;
                    
                case "sendToIngest" :   
                    DAMSIngest damsIngest = new DAMSIngest();
                    damsIngest.ingest(cdis);
                    break;
                
                case "linkToCIS" :
                    LinkCollections linkcollections = new LinkCollections();
                    linkcollections.linkToCIS(cdis);
                    break;
                    
                case "sync" :    
                    MetaData metaData = new MetaData();
                    metaData.sync(cdis, statReport);
                    //sync the imageFilePath.  This essentially should be moved out of metadata sync and be called on its own from the main CDIS
                    ImageFilePath imgPath = new ImageFilePath();
                    imgPath.sync(cdis, statReport);
                    break;
                    
                case "thumbnailSync" :    
                    Thumbnail thumbnail = new Thumbnail();
                    thumbnail.sync(cdis, statReport);
                    break;
                    
                case "genReport" :
                    Report report = new Report();
                    report.generate(cdis);
                    break;
                    
                default:     
                    logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting");
                    return;               
            }
            
 
        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try { if ( cdis.damsConn != null)  cdis.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
            try { if ( cdis.cisConn != null)  cdis.cisConn.close(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( cdis.damsConn != null)  cdis.damsConn.close(); } catch (Exception e) { e.printStackTrace(); }
        }         
    
    }
  
}
