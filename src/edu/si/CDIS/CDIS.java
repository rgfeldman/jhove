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
import edu.si.CDIS.DAMS.Database.CDISError;
import edu.si.CDIS.DAMS.Database.CDISMap;
import java.io.File;
import java.util.Iterator;
import edu.si.CDIS.utilties.ErrorLog;


public class CDIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    public Connection cisConn;
    public Connection damsConn;
    public String operationType;
    public Properties properties;
    public HashMap <String,String[]> xmlSelectHash;
    Long batchNumber;
    Handler fh;
            
    public Long getBatchNumber () {
        return this.batchNumber;
    }
    
    private void setBatchNumber (Long batchNumber) {
        this.batchNumber = batchNumber;
    }
    
    private void setOperationType (String operationType) {
        this.operationType = operationType;
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
            
            //send all properties to the logfile
            for (String key : properties.stringPropertyNames()) {
                String value = properties.getProperty(key);
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
             logger.log(Level.FINER, "Error: obtaining Batch number", e );
             return false;
        }
        
        return true;
    }
    
    /*  Method :        verifyProps
        Arguments:      
        Description:    verifies the required properties have been loaded
        RFeldman 7/2015
    */
    private boolean verifyProps (Properties properties) {
        
        //verify global properties exist in config file
            String[] requiredProps = {"siHoldingUnit",
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
            
            boolean batchNumberSet = cdis.calcBatchNumber();
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
                    
                    //If we want to link right after the ingest, make sure the file has made it to DAMS...then ingest
                    if (cdis.properties.getProperty("linkAfterIngest").equals("true") ){   
                    
                        // pause for a while...Then run the link operation type after the ingest is complete                   
                    
                        for(Iterator<String> iter = damsIngest.distinctHotFolders.iterator(); iter.hasNext();) {
                            
                            String currentHotFolderName = iter.next();
                            String hotFolderdirName =  cdis.properties.getProperty("hotFolderBaseDir") + "\\" + currentHotFolderName + "\\MASTER";
                            
                            File hotFolderMasterDir = new File (hotFolderdirName);
                            while (hotFolderMasterDir.list().length>0) {
                                logger.log(Level.FINER, "HotFolder Master Directory is not empty.  Wait a few minutes to see if files from this batch are ingested");
                
                                try {
                                    Thread.sleep(150000);
                                } catch (Exception e) {
                                    logger.log(Level.FINER, "Exception in sleep ", e);
                                }
                            }
                        }
                        
                        //Loop through the staging for the current batch
                        for(Iterator<String> iter = damsIngest.distinctHotFolders.iterator(); iter.hasNext();) {   
                        
                            String currentHotFolderName = iter.next();
                            String stagingFolderdirName =  cdis.properties.getProperty("stagingFolderBaseDir") + "\\" + currentHotFolderName + "\\MASTER";
                                
                            File stagingFolder = new File (stagingFolderdirName);                    
                            while (stagingFolder.list().length>0) {
 
                                logger.log(Level.FINER, "Staging Directory is not empty.  Wait a few minutes to see if files from this batch are ingested");
                
                                try {
                                    Thread.sleep(150000);
                                } catch (Exception e) {
                                    logger.log(Level.FINER, "Exception in sleep ", e);
                                }
                            }
                        }
                        
                        //Look for failures in the staging folder area
                        for(Iterator<String> iter = damsIngest.distinctHotFolders.iterator(); iter.hasNext();) {
                            String currentHotFolderName = iter.next();
                            String failedFolderdirName =  cdis.properties.getProperty("stagingFolderBaseDir") + "\\" + currentHotFolderName + "\\FAILED";
                            
                            File failedFolder = new File (failedFolderdirName);                    
                            File[] failedFiles = failedFolder.listFiles();
                            
                            for (int i = 0; i < failedFiles.length; i++) {
 
                                String failedFileName = failedFiles[i].getName();
                
                                CDISMap cdisMap = new CDISMap();
                                cdisMap.setFileName(failedFileName);
                                cdisMap.setBatchNumber(cdis.batchNumber);
                                
                                cdisMap.populateIDForFileBatch(cdis.damsConn);
                                
                                ErrorLog errorLog = new ErrorLog();                                
                                errorLog.capture(cdisMap, "IPE", "Ingest process error for filename: " + failedFileName, cdis.damsConn );          
                                 
                            }
                            
                        }
                    
                        //discontinue the ingestLog handler before we switch to the link process
                        logger.removeHandler(cdis.fh);
                    
                        //Execute the Link Process now that the files have been ingested
                        String[] Arguments = new String[]{"linkToCIS"};
                        CDIS.main(Arguments);
                    
                    }
                    
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
