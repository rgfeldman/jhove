/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools;

import com.artesia.common.encryption.encryption.EncryptDecrypt;
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlReader;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.SimpleFormatter;
import java.util.ArrayList;
import java.util.Properties; 
import java.nio.file.DirectoryStream;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.ZoneId;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.DefaultParser;
import static java.time.temporal.ChronoUnit.DAYS;


public class DamsTools {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private static String application;
    private static Long batchNumber;
    private static String configFile;
    private static Connection cisConn;
    private static Connection damsConn;
    private static String directoryName;
    private static String projectCd;
    private static String operationType;
    private static Properties properties;
    private static String subOperation;
    private static ArrayList <XmlData> xmlQueryDataObjList;
    
    private App app;
    private Operation operation;
    private XmlReader xmlReader;
    private final ZoneId zoneId = ZoneId.of("America/New_York");
    
    public static String getApplication() {
        return DamsTools.application;
    }
           
    public static Long getBatchNumber() {
        return DamsTools.batchNumber;
    }
    
    public static Connection getCisConn() {
            return DamsTools.cisConn;
    }
    
    public static Connection getDamsConn() {
        return DamsTools.damsConn;
    }
    
    public static String getDirectoryName() {
       return DamsTools.directoryName;
    }
    
    public static String getOperationType() {
        return DamsTools.operationType;
    }
    
    public static String getProjectCd() {
        return DamsTools.projectCd;
    }
    
    public static String getSubOperation() {
        return DamsTools.subOperation;
    }
    
    public static ArrayList <XmlData> getSqlQueryObjList() {
        return DamsTools.xmlQueryDataObjList;
    }
    
    public static String getProperty (String property) {
        return DamsTools.properties.getProperty(property);
    }

    private void setBatchNumber (Long batchExecutionNumber) {
        DamsTools.batchNumber = batchExecutionNumber;
    }

    
    /*  Method :        connectToDatabases
        Arguments:      
        Description:    establishes connections to both DAMS and CIS database s
        RFeldman 2/2015
    */
    private Connection connectToDatabases (String dbName) {
        
        Connection dbConn = null;
        
        try {
            //connect to DAMS DB
                //connect to CIS DB
                String passwd = EncryptDecrypt.decryptString(DamsTools.getProperty(dbName + "Pass"));
            
                Class.forName(DamsTools.getProperty(dbName + "Driver"));
                dbConn = DriverManager.getConnection(DamsTools.getProperty(dbName + "ConnString"), 
                                DamsTools.getProperty(dbName +"User"), passwd);
            
                if (DamsTools.getProperty("cisDbAutoCommit") != null && 
                        DamsTools.getProperty("cisDbAutoCommit").equals("true") && 
                        dbName.equals("cis") ) {
                    dbConn.setAutoCommit(true);
                    logger.log(Level.INFO, "Setting autocommit to true");
                }
                else {
                    dbConn.setAutoCommit(false);
                }

                logger.log(Level.INFO, "Connection to " + dbName + " database established.");
        }
            
	catch (Exception e) 
        {
            logger.log(Level.SEVERE,"Failed to connect to DB:" + e.getMessage() + "\n");
        }
        
        return dbConn;
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
            fh = new FileHandler(DamsTools.directoryName + "/log/" + DamsTools.application + "Log-" + DamsTools.operationType + "_" + DamsTools.configFile + "_" + DamsTools.batchNumber + ".txt");
            
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
        
        String iniFile = DamsTools.directoryName + "/conf/" + DamsTools.configFile + ".ini";
        
        logger.log(Level.FINER, "Loading ini file: " + iniFile);
                
        try (FileInputStream fis = new FileInputStream(iniFile)) {
            
            DamsTools.properties.load(fis);
            logger.log(Level.FINER, "Ini File loaded");
            
            //send all properties to the logfile
            for (String key : DamsTools.properties.stringPropertyNames()) {
                String value = DamsTools.properties.getProperty(key);
                logger.log(Level.FINER, "Property: " + key + " value: " + value);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: obtaining config information", e );
            return false;
        }
        
        return true;
    }
    
    /*  Method :        calcBatchExecutionNumber
        Arguments:      
        Description:    calculates and assigns a batch Execution number based on date/time to uniquely define this execution batch
        RFeldman 7/2015
    */
    private boolean calcBatchExecutionNumber () {    
        try {
        
            DateFormat df = new SimpleDateFormat("yyyyMMddkkmmssSSS");
            
            Random rand = new Random();
            String batchNum = df.format(new Date()) + rand.nextInt(100);
            
            setBatchNumber (Long.parseLong(batchNum));
            
        } catch (Exception e) {
              System.out.println("Error: obtaining Batch Execution number" + e );
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
        ArrayList<String> reqProps = new ArrayList<>(); 
        reqProps = operation.returnRequiredProps();
        
        reqProps.add("damsDriver");
        reqProps.add("damsPass");
        reqProps.add("damsUser");
        
        if (reqProps != null) {
            for(String reqProp : reqProps) {
                if(! DamsTools.properties.containsKey(reqProp)) {
                    logger.log(Level.SEVERE, "Missing required property: {0}", reqProp);
                    return false;
                }
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
        
        Path directoryPath = Paths.get(DamsTools.directoryName).resolve(folder);
        
        try {
            DirectoryStream<Path> fileListing = Files.newDirectoryStream( directoryPath, fileNamePrefix + "*.{txt,rtf}" );
        
            for (Path fileName : fileListing) { 
                BasicFileAttributes attr = Files.readAttributes(fileName, BasicFileAttributes.class);
                
                if ( attr.creationTime().toInstant().atZone(zoneId).compareTo(Instant.now().minus(numDays, DAYS).atZone(zoneId)) < 0 ) {
                    //System.out.println("Removing file: " + fileName.getFileName().toString());
                    Files.delete(fileName);
                }                   
            }
        
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void applicationFactory() {
        
        switch (DamsTools.application) {
            case "CDIS":
                app = new Cdis();
                break;
            case "VFCU":   
                app = new Vfcu();
                break;
        }      
    }

    /*  Method :        main
        Arguments:      
        Description:    starting point of the CDIS application
        RFeldman 2/2015
    */
    public static void main(String[] args) {
       
        DamsTools damsTool = new DamsTools();
        
        damsTool.handleArguments(args);  
        damsTool.applicationFactory();
        if (damsTool.app == null) {
            System.out.println( "invalid damsTool specified");
        }
  
        // Delete old log and report files
        damsTool.deleteLogs("rpt", damsTool.application + "",21);
        damsTool.deleteLogs("log", damsTool.application + "",21);
        
        try {
            DamsTools.properties = new Properties();
            
            boolean batchExecutionNumberSet = damsTool.calcBatchExecutionNumber();
            if (! batchExecutionNumberSet) {
                System.out.println("Fatal Error: Batch Execution number could not be generated");
                return;
            }
            
            //set the logger
            boolean loggingSet = damsTool.setLogger();
            if (! loggingSet) {
                System.out.println("Fatal Error: Failure to set Logger, exiting");
                return;
            }
            
            //handle the ini file
            boolean iniRead = damsTool.readIni ();
            if (! iniRead) {
                logger.log(Level.SEVERE, "Fatal Error: Failure to Load ini file, exiting");
                return;
            }
            
            DamsTools.projectCd = DamsTools.getProperty("projectCd");
            damsTool.operation = damsTool.app.operationFactiory();
            
            boolean propsVerified = damsTool.verifyProps ();
            if (! propsVerified) {
                logger.log(Level.SEVERE, "Fatal Error: Required Property missing.  Exiting");
                return;
            }
            
            damsConn = damsTool.connectToDatabases("dams");
            if (damsConn == null ) {
                logger.log(Level.SEVERE, "Fatal Error: unable to connect to damsDb.  Exiting");
                return;
            }

            if (DamsTools.getProperty("cisDriver") != null) {
                cisConn = damsTool.connectToDatabases("cis");
                if (cisConn == null ) {
                    logger.log(Level.SEVERE, "Fatal Error: unable to connect to cisDb. Exiting");
                    return;
                }
            } 
            
            
            if (damsTool.operation.requireSqlCriteria() ) {
                
                damsTool.populateSqlXmlQueryDataList();
             
            }
            
            damsTool.operation.invoke();

            logger.log(Level.INFO, DamsTools.getOperationType() + " Complete");
            
        } catch (Exception e) {
                e.printStackTrace();
        } finally {
            try { if ( DamsTools.cisConn != null)  DamsTools.cisConn.close(); } catch (Exception e) { e.printStackTrace(); }
            try { if ( DamsTools.damsConn != null)  DamsTools.damsConn.close(); } catch (Exception e) { e.printStackTrace(); }
        }         
    }
    
    private void populateSqlXmlQueryDataList () {
        
        DamsTools.xmlQueryDataObjList = new ArrayList();
        Path xmlFile = Paths.get(DamsTools.directoryName);
        
        if (DamsTools.getSubOperation() == null ) {
            xmlFile = xmlFile.resolve(DamsTools.getProperty(DamsTools.getOperationType() + "XmlFile"));
                    
            logger.log(Level.FINEST, "Looking for xml at: " + xmlFile );
                    
            xmlReader = new XmlReader(xmlFile.toString(), DamsTools.getOperationType());
        }
        else {
            xmlFile = xmlFile.resolve(DamsTools.getProperty(DamsTools.getOperationType() + "-" + DamsTools.getSubOperation() + "XmlFile"));
                                   
            logger.log(Level.FINEST, "Looking for xml at: " + xmlFile );
                    
            xmlReader = new XmlReader(xmlFile.toString(), DamsTools.getOperationType(), DamsTools.getSubOperation());
        }                  
        xmlQueryDataObjList = xmlReader.parseReturnXmlObjectList();
                
    }
    
    private void handleArguments (String[] args) {
        
        Options options = new Options();
        Option option;
        
        option = Option.builder("a")
            .hasArg()
            .required()
            .argName("application")
            .build();
        options.addOption( option );
        
        option = Option.builder("c")
            .hasArg()
            .required()
            .argName("configFile")
            .build();
        options.addOption( option );
        
        option = Option.builder("d")
            .hasArg()
            .required()
            .argName("directoryName")
            .build();
        options.addOption( option );
        
        option = Option.builder("o")
            .hasArg()
            .required()
            .argName("operationType")
            .build();
        options.addOption( option );
        
        option = Option.builder("s")
            .hasArg()
            .argName("subOperation")
            .build();
        options.addOption( option );
        
        // create the parser
        CommandLineParser parser = new DefaultParser();
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            
            DamsTools.application = line.getOptionValue("a");
            DamsTools.configFile = line.getOptionValue( "c" );
            DamsTools.directoryName = line.getOptionValue( "d" );
            DamsTools.operationType = line.getOptionValue("o");
            DamsTools.subOperation = line.getOptionValue("s");
        }
        catch( Exception e ) {
            // oops, something went wrong
            System.err.println( "Parsing failed.  Reason: " + e.getMessage() );
        }
    }
}
