/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.utilties.XmlSqlConfig;
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
import edu.si.CDIS.CIS.Thumbnail;
import java.util.HashMap;
import com.artesia.common.encryption.encryption.EncryptDecrypt;
import java.io.File;


public class CDIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    private static Connection cisConn;
    private static Connection damsConn;
    public String operationType;
    private static Properties properties;
    private static HashMap <String,String[]> xmlSelectHash;
    private static Long batchNumber;
    
    private void setOperationType (String operationType) {
        this.operationType = operationType;
    }
    
    public static HashMap<String, String[]> getXmlSelectHash() {
        return CDIS.xmlSelectHash;
    }
    
    public static Connection getCisConn() {
        return CDIS.cisConn;
    }
    
    public static Connection getDamsConn() {
        return CDIS.damsConn;
    }
        
    public static Long getBatchNumber() {
        return CDIS.batchNumber;
    }
        
    public static Properties getProperties() {
        return CDIS.properties;
    }
    
    public static String getProperty (String property) {
        return CDIS.properties.getProperty(property);
    }

    private void setBatchNumber (Long batchNumber) {
        CDIS.batchNumber = batchNumber;
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
            String damsPass = EncryptDecrypt.decryptString(CDIS.getProperty("damsPass"));
            
            CDIS.damsConn = DataProvider.establishConnection(CDIS.getProperty("damsDriver"), 
					CDIS.getProperty("damsConnString"), 
					CDIS.getProperty("damsUser"), 
					damsPass);
                
        } catch(Exception ex) {
		logger.log(Level.SEVERE, "Failure Connecting to DAMS database.", ex);
		return false;
	}
        
        logger.log(Level.FINER, "Connection to DAMS database established.");
        
        
        try {
                if (CDIS.getProperty("cisSourceDB").equals("none")) {
                    
                    logger.log(Level.FINER, "CIS source is CDISDB. No connection to CIS database needed.");
                    
                } else {
                    //decrypt the password, it is stored in ini in encypted fashion
                    String tmsPass = EncryptDecrypt.decryptString(CDIS.getProperty("cisPass"));
                
                    CDIS.cisConn = DataProvider.establishConnection(CDIS.getProperty("cisDriver"), 
                    CDIS.getProperty("cisConnString"), 
                    CDIS.getProperty("cisUser"), 
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
        Handler fh;
            
        try {
		fh = new FileHandler("log\\CDISLog-" + this.operationType + CDIS.batchNumber + ".txt");
	
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
        
        String iniFile = "conf\\cdis.ini";
        
        logger.log(Level.FINER, "Loading ini file: " + iniFile);
                
        try (FileInputStream fis = new FileInputStream(iniFile)) {
            
            CDIS.properties.load(fis);
            logger.log(Level.FINER, "Ini File loaded");
            
            //send all properties to the logfile
            for (String key : CDIS.properties.stringPropertyNames()) {
                String value = CDIS.properties.getProperty(key);
                logger.log(Level.FINER, "Property: " + key + " value: " + value);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: obtaining config information", e );
            return false;
        }
        
        return true;
    }
    
    /*  Method :        calcBatchNumber
        Arguments:      
        Description:    calculates and assigns a batch number based on date/time to uniquely define this execution batch
        RFeldman 7/2015
    */
    private boolean calcBatchNumber () {    
        try {
        
            DateFormat df = new SimpleDateFormat("yyyyMMddkkmmss");
            setBatchNumber (Long.parseLong(df.format(new Date())));
            
        } catch (Exception e) {
              System.out.println("Error: obtaining Batch number" + e );
             return false;
        }
        return true;
    }
    
    /*  Method :        verifyProps
        Arguments:      
        Description:    verifies the required properties have been loaded
        RFeldman 7/2015
    */
    private boolean verifyProps () {
        
        //verify global properties exist in config file
            String[] requiredProps = {"siHoldingUnit",
                                    "damsDriver",
                                    "damsConnString",
                                    "damsUser",
                                    "damsPass",
                                    "cisSourceDB",
                                    "collectionGroup",
                                    "xmlSQLFile"};
        
        for(int i = 0; i < requiredProps.length; i++) {
            String reqProp = requiredProps[i];
            if(! CDIS.properties.containsKey(reqProp)) {
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
        
        // Delete old log and report files
        cdis.deleteLogs("rpt","CDISRPT-",14);
        cdis.deleteLogs("log","CDISLog-",14);
        
        // Check if the required number of arguments are inputted
        if(args.length < 1) {
            System.out.println("Missing parameter: <operationType>");
            return;
	}
	else {
            cdis.setOperationType(args[0]);
	}

        try {
        
            CDIS.properties = new Properties();
              
            boolean batchNumberSet = cdis.calcBatchNumber();
            if (! batchNumberSet) {
                 System.out.println("Fatal Error: Batch number could not be generated");
                return;
            }
            
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
            
            boolean propsVerified = cdis.verifyProps ();
            if (! propsVerified) {
                logger.log(Level.SEVERE, "Fatal Error: Required Property missing.  Exiting");
                return;
            }
        
            boolean databaseConnected = cdis.connectToDatabases();
            if (! databaseConnected) {
                logger.log(Level.SEVERE, "Fatal Error: Failure to Connect to database");
                return;
            }
            

            
            // read the XML config file and obtain the selectStatements
            XmlSqlConfig xml = new XmlSqlConfig();             
            boolean xmlReturn = xml.read(cdis.operationType, CDIS.getProperty("xmlSQLFile"));
            if (! xmlReturn) {
                logger.log(Level.SEVERE, "Fatal Error: unable to read/parse sql xml file");
                return;
            }
            
            CDIS.xmlSelectHash = new HashMap <>(xml.getSelectStmtHash());
            
            switch (cdis.operationType) {
                
                case "sendToHotFolder" :   
                    SendToHotFolder sendToHotFolder = new SendToHotFolder();
                    sendToHotFolder.ingest();  
                    break;
                    
                case "linkToDamsToCis" :
                    LinkDamsToCIS linkDamsToCis = new LinkDamsToCIS();
                    linkDamsToCis.link();
                    break;
                            
                case "linkToCISMdpp" :
                    LinkToCISMdpp linkToCisMdpp = new LinkToCISMdpp();
                    linkToCisMdpp.link();
                    break;
                                    
                case "linkToDAMS" :
                    LinkToDAMS linkToDams = new LinkToDAMS();
                    linkToDams.link();
                    break;

                case "metaDataSync" :    
                    MetaDataSync metaData = new MetaDataSync();
                    metaData.sync();
                    break;
                    
                case "createCISmedia" :
                    CreateCISmedia createCisMedia = new CreateCISmedia();
                    createCisMedia.createMedia();
                    break;  
                
                case "idsCisSync" :
                    IdsCisSync idsCisSync = new IdsCisSync();
                    idsCisSync.sync();
                    break;
                            
                case "thumbnailSync" :    
                    Thumbnail thumbnail = new Thumbnail();
                    thumbnail.sync();
                    break;
                    
                case "genReport" :
                    Report report = new Report();
                    report.generate();
                    break;
                    
                default:     
                    logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting");
                    return;               
            }
            
 
        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try { if ( CDIS.damsConn != null)  CDIS.damsConn.commit(); } catch (Exception e) { e.printStackTrace(); }
            
            try { if ( CDIS.cisConn != null)  CDIS.cisConn.close(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( CDIS.damsConn != null)  CDIS.damsConn.close(); } catch (Exception e) { e.printStackTrace(); }
        }         
    
    }
  
}
