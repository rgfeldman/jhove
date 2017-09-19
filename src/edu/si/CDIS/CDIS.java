/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.report.Generator;
import com.artesia.common.encryption.encryption.EncryptDecrypt;
import edu.si.Utils.XmlSqlConfig;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties; 
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.SimpleFormatter;
import org.w3c.dom.NodeList;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;


public class CDIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    private static Long batchNumber;
    private static Connection cisConn;
    private static String projectCd;
    private static Connection damsConn;
    private static String operationType;
    private static Properties properties;
    private static String siHoldingUnit;
    private static NodeList queryNodeList;
    private static String configFile;
    
    public static NodeList getQueryNodeList() {
        return CDIS.queryNodeList;
    }
    
    public static String getProjectCd() {
        return CDIS.projectCd;
    }
    
    public static Connection getCisConn() {
        if (CDIS.getProperty("cis").equals("none")) {
            return CDIS.damsConn;
        }
        else if (CDIS.getProperty("linkFromDams") != null && 
                CDIS.getProperty("linkFromDams").equals("true") 
                && operationType.equals("linkToDamsAndCis")) {
            return CDIS.damsConn;
        }
        else 
            return CDIS.cisConn;
    }
    
    public static Connection getDamsConn() {
        return CDIS.damsConn;
    }
        
    public static Long getBatchNumber() {
        return CDIS.batchNumber;
    }
    
    public static String getOperationType() {
        return CDIS.operationType;
    }
        
    public static Properties getProperties() {
        return CDIS.properties;
    }
    
    public static String getProperty (String property) {
        return CDIS.properties.getProperty(property);
    }

    public static String getSiHoldingUnit() {
        return CDIS.siHoldingUnit;
    }

    private void setBatchNumber (Long batchNumber) {
        CDIS.batchNumber = batchNumber;
    }
    
    
    /*  Method :        connectToDatabases
        Arguments:      
        Description:    establishes connections to both DAMS and CIS database s
        RFeldman 2/2015
    */
    private boolean connectToDatabases () {
        
        try {
            //connect to DAMS DB
            String passwd = EncryptDecrypt.decryptString(CDIS.getProperty("damsPass"));
              
            Class.forName(CDIS.getProperty("damsDriver"));
            CDIS.damsConn = DriverManager.getConnection(CDIS.getProperty("damsConnString"), 
                                CDIS.getProperty("damsUser"), passwd);
            
            logger.log(Level.INFO, "Connection to DAMS database established.");
            
            if (CDIS.getProperty("cis").equals("none")) {
                logger.log(Level.INFO, "No CIS database sepecified, skipping connection to CIS");
                return true;
            }
            
            if (CDIS.operationType.equals("linkToDamsAndCis") && ! (CDIS.getProperty("linkFromDams") == null) 
                    && CDIS.getProperty("linkFromDams").equals("true") ) {
                logger.log(Level.INFO, "No CIS connection needed, skipping connection to CIS");
            }
            
            //connect to CIS DB
            passwd = EncryptDecrypt.decryptString(CDIS.getProperty("cisPass"));
            
            Class.forName(CDIS.getProperty("cisDriver"));
            CDIS.cisConn = DriverManager.getConnection(CDIS.getProperty("cisConnString"), 
                                CDIS.getProperty("cisUser"), passwd);
            
           
                
            logger.log(Level.INFO, "Connection to CIS database established.");
            
	}
            
	catch (Exception e) 
        {
            logger.log(Level.SEVERE,"Failed to connect to DB:" + e.getMessage() + "\n");
            return false;
        }
        
        return true;
        
    }
    
    private boolean setDbDefaults () {
        try { 
            
            if ( CDIS.damsConn != null) {
                CDIS.damsConn.setAutoCommit(false);
            }
             
            if ( CDIS.cisConn != null) {
                CDIS.cisConn.setAutoCommit(false);                        
            }
 
        } catch (Exception e) 
            {
                logger.log(Level.SEVERE,"Failed to connect to DB:" + e.getMessage() + "\n");
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
            fh = new FileHandler(CDIS.siHoldingUnit + "/log/CDISLog-" + CDIS.operationType + "_" + this.configFile + "_" + CDIS.batchNumber + ".txt");
            
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
    
    /*  Method :        readIni
        Arguments:      
        Description:    assigns values from the .ini file into the properties object
        RFeldman 2/2015
    */
    private boolean readIni () {
        
        String iniFile = CDIS.siHoldingUnit + "/conf/" + CDIS.configFile + ".ini";
        
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
        
            DateFormat df = new SimpleDateFormat("yyyyMMddkkmmssSSS");
            
            Random rand = new Random();
            String batchNum = df.format(new Date()) + rand.nextInt(100);
            
            setBatchNumber (Long.parseLong(batchNum));
            
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
            String[] requiredProps = {"damsDriver",
                                    "damsConnString",
                                    "damsUser",
                                    "damsPass",
                                    "cis"};
        
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
                
        File logfolderDir = new File(CDIS.siHoldingUnit + "/" + folder);
        
        File[] logs = logfolderDir.listFiles();
	
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
        
        cdis.handleArguments(args);
        
        // Delete old log and report files
        cdis.deleteLogs("rpt","CDISRPT-",21);
        cdis.deleteLogs("log","CDISLog-",21);
        
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
            
            CDIS.projectCd = CDIS.getProperty("projectCd");
            
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
            
            boolean dbDefaultsSet = cdis.setDbDefaults();
            if (! dbDefaultsSet) {
                logger.log(Level.SEVERE, "Fatal Error: Unable to set Database Defaults");
                return;
            }
            
            // read the XML config file
            XmlSqlConfig xml = new XmlSqlConfig();
            xml.setFileNameAndPath(siHoldingUnit + "/" + CDIS.getProperty(CDIS.operationType + "XmlFile"));
            boolean xmlReturn = xml.read(CDIS.getOperationType());
            if (! xmlReturn) {
                logger.log(Level.SEVERE, "Fatal Error: unable to read/parse sql xml file");
                return;
            }
            // save the queries in a Node List
            CDIS.queryNodeList = xml.getOpQueryNodeList();
            
            switch (CDIS.operationType) {
                
                case "sendToHotFolder" :   
                    SendToHotFolder sendToHotFolder = new SendToHotFolder();
                    sendToHotFolder.sendForingest(); 
                    break;
                    
                case "linkToDamsAndCis" :
                    LinkToDamsAndCIS linkDamsAndCis = new LinkToDamsAndCIS();
                    linkDamsAndCis.link();
                    break;
                            
                case "linkToCis" :
                    LinkToCis linkToCis = new LinkToCis();
                    linkToCis.link();
                    break;
                                    
                case "linkToDams" :
                    LinkToDams linkToDams = new LinkToDams();
                    linkToDams.link();
                    break;

                case "metadataSync" :    
                    MetaDataSync metadata = new MetaDataSync();
                    metadata.sync();
                    break;
                    
                case "createCisMedia" :
                    CreateCisMedia createCisMedia = new CreateCisMedia();
                    createCisMedia.createMedia();
                    break;  
                
                case "idsCisSync" :
                    IdsCisSync idsCisSync = new IdsCisSync();
                    idsCisSync.sync();
                    break;
                            
                case "thumbnailSync" :    
                    CISThumbnailSync thumbnail = new CISThumbnailSync();
                    thumbnail.sync();
                    break;
                    
                case "cisUpdate" :
                    CisUpdate cisUpdate = new CisUpdate();
                    cisUpdate.updateCisFromDams();
                    break;
                    
                 case "vfcuDirReport" :
                 case "timeframeReport" :    
                    Generator generator = new Generator();
                    generator.invoke();
                    break;
                    
                default:     
                    logger.log(Level.SEVERE, "Fatal Error: Invalid Operation Type, exiting"); 
                    return;
            }
            
            logger.log(Level.INFO, CDIS.getOperationType() + " Complete");
 
        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try { if ( CDIS.cisConn != null)  CDIS.cisConn.close(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( CDIS.damsConn != null)  CDIS.damsConn.close(); } catch (Exception e) { e.printStackTrace(); }
        }         
    
    }
    
    private void handleArguments (String[] args) {
        
        Options options = new Options();
        
        Option option = Option.builder("o")
            .hasArg()
            .required()
            .argName("operationType")
            .build();
        options.addOption( option );
        
        option = Option.builder("c")
            .hasArg()
            .required()
            .argName("configFile")
            .build();
        options.addOption( option );
        
        option = Option.builder("s")
            .hasArg()
            .required()
            .argName("siHoldingUnit")
            .build();
        options.addOption( option );
        
        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            
            CDIS.operationType = line.getOptionValue("o");
            
            this.configFile = line.getOptionValue( "c" );

            CDIS.siHoldingUnit = line.getOptionValue( "s" );
            
        }
        catch( Exception e ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
        }
    }
  
}
