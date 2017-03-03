/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISErrorLog;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.VFCUMd5File;
import com.lowagie.text.*;
import com.lowagie.text.rtf.RtfWriter2;
import com.lowagie.text.rtf.style.RtfFont;
import edu.si.Utils.XmlSqlConfig;
import java.io.File;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class GenVfcuDirReport {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    private LinkedHashMap<Integer, String> completedIdName;
    private String completedStepSql;
    private Document document;
    private ArrayList<Integer> masterMd5Ids;
    private LinkedHashMap<Integer, String> failedIdName;
    private String rptFile;
    private String rptVendorDir;
    private String statsHeader;
       
    
    /*  Method :       countFilesInDeliveryLoc
        Arguments:      
        Description:    Counts number of files that exist in the delivery location
        RFeldman 2/2017
    */
    int countFilesInDeliveryLoc(String deliveryLocation) {
        int numFiles = 0;
        
        File deliveryDirectory = new File(deliveryLocation);
        
        //We count the files unless they end with .txt (which could indicate a ready file)
        for (File file : deliveryDirectory.listFiles()) {
            if (! file.getName().endsWith("txt")) {
                numFiles++;
            }
        }
        
        return numFiles;
    }

    /*  Method :       countNumberFilesDbDelivered
        Arguments:      
        Description:    Counts number of files that have been marked as delivered to post-ingest area in the Database
        RFeldman 2/2017
    */
    int countNumberFilesDbDelivered (Integer masterMd5Id, Integer childMd5Id) {
        int numFilesSent = 0;
        
        String sql = "SELECT COUNT (*) " +
                     "FROM vfcu_media_file media " +
                     "INNER JOIN vfcu_activity_log activity" +
                     "ON media.vfcu_media_file_id = activity.vfcu_media_file_id " + 
                     "AND media.vfcu_md5_file_id in (" + masterMd5Id + ", " + childMd5Id + ")";
        
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            if (rs.next()) {
                numFilesSent = rs.getInt(1);
            }
            
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to count list of pending records", e);
            numFilesSent = -1;
	}
        
        return numFilesSent;
    }
    
    /*  Method :       countPendingRecords
        Arguments:      
        Description:    Counts number of files that are pending and still have processing to go befroe being ready for reporting
        RFeldman 2/2017
    */
    int countPendingRecords (Integer masterMd5Id, Integer childMd5Id) {
        int numPendingRecords = 0;
        
        String queryParam = null;
        if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            if (CDIS.getProperty("rptStatus").equals("LCC") ) {
                // If we are using master/child pairs, typically only the child record goes to the CIS, so only child will have LCC
                queryParam = "WHERE  a.vfcu_md5_file_id = " + childMd5Id;   
            }
            else {
                // If we are using master/child pairs, and are interested in a report of LDC, we want both master and children in report
                queryParam = "WHERE  a.vfcu_md5_file_id in (" + masterMd5Id + ", " + childMd5Id+ ") ";
            }
        }
        else {
            // We do not have a child record, only look at master record
            queryParam = "WHERE  a.vfcu_md5_file_id = " + masterMd5Id;       
        }
        
        String sql = "SELECT COUNT(*) " +
                     "FROM   vfcu_media_file a " +
                     queryParam +
                     " AND  NOT EXISTS ( " +
                     "  SELECT 'X' " +
                     "  FROM   cdis_map b, " +
                     "         cdis_activity_log c " +
                     "  WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "  AND b.cdis_map_id = c.cdis_map_id " +
                     "  AND   c.cdis_status_cd in ('" + CDIS.getProperty("rptStatus") + "','ERR'))" +
                     "  AND  NOT EXISTS ( " +  
                     "  SELECT 'X'     " +
                     "  FROM   vfcu_activity_log d " +     
                     "  WHERE a.vfcu_media_file_id = d.vfcu_media_file_id " +   
                     "  AND   d.vfcu_status_cd in ('ER','OH')) ";
        
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            if (rs.next()) {
                numPendingRecords = rs.getInt(1);
            }         
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to count list of pending records", e);
            numPendingRecords = -1;
	}
        
        return numPendingRecords;
    }
    
    
    /*  Method :       countVerify
        Arguments:      
        Description:    Calls several other methods that perform counts, and determines if the current report should be marked as completed
        RFeldman 2/2017
    */
    private boolean countVerify(Integer masterMd5FileId, Integer childMd5FileId, String deliveryLocation) {
        //Perform counts to see status
        int numPendingRecords = 0;
        int numFilesDbDelivered = 0;
        int numFilesInDeliveryArea = 0;
        
        countPendingRecords(masterMd5FileId, childMd5FileId );
        
        if (! (numPendingRecords == 0)) {
            logger.log(Level.FINEST, "Some records are in pending state...holding off for report generation.");
            return false;
        }
            
        //See if we are sending files to the postIngest destination location
        if  (CDIS.getProperty("postIngestDeliveryLoc") != null) {
            //Count number of files marked as having been sent to ingest
            numFilesDbDelivered = countNumberFilesDbDelivered(masterMd5FileId, childMd5FileId);
                
            // Count the number of files in the physical location   
            numFilesInDeliveryArea = countFilesInDeliveryLoc (deliveryLocation);   
        
            if (numFilesDbDelivered != numFilesInDeliveryArea) {
                logger.log(Level.FINEST, "All Files in DB are not in delivery location or vice versa");
                logger.log(Level.FINEST, "Num Files listed in DB: ", numFilesDbDelivered);
                logger.log(Level.FINEST, "Num Files in Delivery Area: ", numFilesInDeliveryArea);
                return false;
            }
        }
        
        return true;
    }
    
    /*  Method :       create
        Arguments:      
        Description:   Creates the report document itself
        RFeldman 2/2017
    */
    private boolean create () {
        
        String timeStamp;
        String timeStampWords;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());

        this.rptFile =  CDIS.getProjectCd() + "\\rpt\\CDISRPT-" + CDIS.getProjectCd() + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            
            document.add(new Paragraph(timeStampWords + "\n" + 
                CDIS.getProjectCd() + " CDIS Activity Report- " + this.rptVendorDir, title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report ", e);
            return false;
        }  
        
        return true;
    }

    /*  Method :       createReadyFile
        Arguments:      
        Description:   Physically creates the ready file in the pickup location
        RFeldman 2/2017
    */
    private void createReadyFile (String postIngestDeliveryLoc) {
        
        try {
                //Create the ready.txt file and put in the media location
                String readyFilewithPath = postIngestDeliveryLoc + "\\" + CDIS.getProperty("readyFileName");

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready file",e);;
        }
    }
    
    /*  Method :       generate
        Arguments:      
        Description:   The main driver for the current class that creates vfcudirectory level reports
        RFeldman 2/2017
    */
    public void generate () {
        
        masterMd5Ids = new ArrayList<> () ;
        
        //get masterMd5FileId to run report for (FROM XML)
        populateMasterMd5FileIds();
   
        if (masterMd5Ids.isEmpty() ) {
            logger.log(Level.FINEST, "No md5 file obtained, or none to report meeting condition...cannot generate report.");
            return;
        }
        
        // NEED TO LOOP THROUGH MD5 IDs one at a time
        for (Integer masterMd5Id : masterMd5Ids) {
            
            VFCUMd5File masterVfcuMd5File = new VFCUMd5File();
            VFCUMd5File childVfcuMd5File = new VFCUMd5File();
        
            masterVfcuMd5File.setMasterMd5FileId(masterMd5Id);
            masterVfcuMd5File.setVfcuMd5FileId(masterMd5Id);
            childVfcuMd5File.setMasterMd5FileId(masterMd5Id);
            
            if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            //get childMd5FileId for the master XML
            
                childVfcuMd5File.setVfcuMd5FileId (childVfcuMd5File.returnSubFileMd5Id());
                        
                if (childVfcuMd5File.getVfcuMd5FileId() == null) {
                    logger.log(Level.FINEST, "No child md5 file obtained, or none to report meeting condition...cannot generate report.");
                    continue;
                }
            }
        
            String pickupLocation = null;
            
            if (CDIS.getProperty("postIngestDeliveryLoc") != null) {
                pickupLocation = CDIS.getProperty("postIngestDeliveryLoc") + "\\" + masterVfcuMd5File.getFilePathEnding();
            }
            
            masterVfcuMd5File.setFilePathEnding();
            
            boolean countsVerified = countVerify(masterVfcuMd5File.getVfcuMd5FileId(), childVfcuMd5File.getVfcuMd5FileId(), pickupLocation);
            if (! countsVerified) {
                logger.log(Level.FINEST, "Not everything complete, will check next directory");
                continue;
            }
            
            //get the vendordir name for the report
            populateRptVendorDir(masterVfcuMd5File.getVfcuMd5FileId());
                
            //if both master and child md5 file are complete, generate the detailed lists. 
            create();
        
            //Get list of completed records (UOI_IDs) from the past increment
            this.completedIdName = new LinkedHashMap<>();
            genCompletedIdList (masterVfcuMd5File.getVfcuMd5FileId(), childVfcuMd5File.getVfcuMd5FileId());
     
            //Get the failed records from the past increment
            this.failedIdName = new LinkedHashMap<>();
            genFailedIdList (masterVfcuMd5File.getVfcuMd5FileId(), childVfcuMd5File.getVfcuMd5FileId());
        
            //only continue if the completedList and FailedList have reoords
            if (this.completedIdName.isEmpty() && this.failedIdName.isEmpty() ) {
                logger.log(Level.FINEST, "No Rows to report, will get next report");
                continue;
            }
        
            statisticsGenerate();
        
            try {
                RtfFont stats=new RtfFont("Times New Roman",12);
                document.add(new Paragraph(this.statsHeader, stats));
            }    
            catch(Exception e) {
                logger.log(Level.FINEST, "Unable to Obtain Header information");
            }
                
            if (CDIS.getProperty("rptVfcudirListFiles").equals("true") ||  (this.failedIdName.size() > 0) ) {
                //failed list is to be displayed before successful list per Ken
                writeFailed();
                
                //now write successful completion list to file
                writeCompleted();
            }
        
            //close the Document
            document.close();
 
            if (CDIS.getProperty("vfcuDirEmailList") != null) { 
                //send email to list
                logger.log(Level.FINEST, "Need To send Email Report");
            
                send();
            }
        
            // If there is a post Ingest file specified, see if we need to create a ready file there
            if (CDIS.getProperty("postIngestDeliveryLoc") != null) {
                createReadyFile(pickupLocation);
            }
            
            //set the report generated flag to indicate report was generated
            masterVfcuMd5File.updateCdisRptDt();
        
            if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
                childVfcuMd5File.updateCdisRptDt();
            }   
            
        }
        
        try { if ( CDIS.getDamsConn()!= null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
    }
   
    
    /*  Method :       genCompletedIdList
        Arguments:      
        Description:   Generates the list of files that have completed that will be listed in the report
        RFeldman 2/2017
    */
    private boolean genCompletedIdList (Integer masterMd5FileId, Integer childMd5FileId) {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getSuccessRecords"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            } 
            String sql = xml.getSqlQuery();
            
            if (sql.contains("?MD5_MASTER_ID?")) {
                sql = sql.replace("?MD5_MASTER_ID?", Integer.toString(masterMd5FileId)) ;
            }
            if (sql.contains("?MD5_CHILD_ID?")) {
                sql = sql.replace("?MD5_CHILD_ID?", Integer.toString(childMd5FileId));
            }
            
            this.completedStepSql = sql;
        
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                     this.completedIdName.put(rs.getInt(1), rs.getString(2)); 
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list of Successful Records, returning", e);
                return false;
            }
            
        }            
        
        return true;
        
    }
    
    /*  Method :       genFailedIdList
        Arguments:      
        Description:   Generates the list of files that have failed processing that will be listed in the report
        RFeldman 2/2017
    */
    private boolean genFailedIdList (Integer masterMd5FileId, Integer childMd5FileId) {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getFailedRecords"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }     
            String sql = xml.getSqlQuery();
            
            if (sql.contains("?MD5_MASTER_ID?")) {
                sql = sql.replace("?MD5_MASTER_ID?", Integer.toString(masterMd5FileId)) ;
            }
            if (sql.contains("?MD5_CHILD_ID?")) {
                sql = sql.replace("?MD5_CHILD_ID?", Integer.toString(childMd5FileId));
            }
        
            
            logger.log(Level.FINEST, "SQL: {0}", sql);
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                    this.failedIdName.put(rs.getInt(1), rs.getString(2));  
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list of Failed Records, returning", e);
                return false;
            }
        }            
        
        return true;
        
    }
    
    
    /*  Method :       populateMasterMd5FileIds
        Arguments:      
        Description:   Generates the list of master md5 fileIds that could be reported on
        RFeldman 2/2017
    */
    private boolean populateMasterMd5FileIds () {
        
        XmlSqlConfig xml = new XmlSqlConfig(); 
        xml.setOpQueryNodeList(CDIS.getQueryNodeList());
        
        //indicate the particular query we are interested in
        xml.setQueryTag("getMasterMd5Ids"); 
        
        //Loop through all of the queries for the current operation type
        for (int s = 0; s < CDIS.getQueryNodeList().getLength(); s++) {
            boolean queryPopulated = xml.populateSqlInfoForType(s);
        
            //if the query does not match the tag, then get the next query
            if (!queryPopulated ) {
                continue;
            }     
            
            logger.log(Level.FINEST, "SQL: {0}", xml.getSqlQuery());
            
            try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(xml.getSqlQuery());
            ResultSet rs = stmt.executeQuery() ) {

                //Add the value from the database to the list
                while (rs.next()) {
                    masterMd5Ids.add(rs.getInt(1));
                }
            }
            catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list of masterMd5s, returning", e);
                return false;
            }
            
        }            
        
        return true;
            
    }
    
    /*  Method :       populateRptVendorDir
        Arguments:      
        Description:   Generates the rptVendorDir which will contain the name of the directory on the report
        RFeldman 2/2017
    */    
    private boolean populateRptVendorDir(Integer masterMd5FileId) {
        
        String sql = null;
        
        if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            sql = "SELECT SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '\\', 1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + masterMd5FileId;
        } 
        else {
            sql = "SELECT file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + masterMd5FileId;
        }
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                String strFilePathEnding = rs.getString(1);
                //Add the records to the masterMd5Id list 
                if (strFilePathEnding.contains("\\")) {
                      this.rptVendorDir = strFilePathEnding.replace("\\", "-");
                }
                else {
                      this.rptVendorDir = strFilePathEnding;
                } 
            }        
            else {
                throw new Exception();
            }
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain vendor dir for report", e);
                return false;
	}
        
        return true;
        
    }
    
    
    /*  Method :       send
        Arguments:      
        Description:   emails the physical report to receipients listed in the configuration file
        RFeldman 2/2017
    */  
    public void send () {
        try {
            Properties mailProps = new Properties();
            String server = "smtp.si.edu";
            mailProps.put("mail.smtp.host", server);      
            Session session = Session.getDefaultInstance( mailProps, null );

            MimeMessage message = new MimeMessage( session );
            message.setFrom(new InternetAddress("no-reply@dams.si.edu"));
            String[] toEmailAddrArray = CDIS.getProperty("vfcuDirEmailList").split(",");
            for (int i = 0; i < toEmailAddrArray.length; i++) {
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
            }
            
            if (CDIS.getProperty("rptStatus").equals("LDC")) {
                message.setSubject(CDIS.getProjectCd()+ ": Batch Hot Folder Import Activity Report - " + this.rptVendorDir);
            }
            else {
                message.setSubject(CDIS.getProjectCd()+ ": Batch Hot Folder Media Creation/Integration Activity Report - " + this.rptVendorDir);
            }
            
            String emailContent = this.statsHeader.replace("\n","<br>");
            
            if (CDIS.getProperty("rptVfcudirListFiles").equals("true") || (this.failedIdName.size() > 0) ) { 
                 emailContent = emailContent + "<br>Please see the attached CDIS Activity Report <br>";
            }     
            
            emailContent = emailContent + "<br><br><br>If you have any questions regarding information contained in this report, please contact: <br>" + 
                    "Robert Feldman (FeldmanR@si.edu) or Isabel Meyer (MeyerI@si.edu) <br><br><br><br>" + 
                    "Please let us know if someone else in your organization requires this information, or if you would like for us to discontinue delivery of this report to you.<br>" +
                    "Thanks! The DAMS team";
            
            // create the Multipart and its parts to it
            Multipart parts = new MimeMultipart();

            // create and fill the Body
           
            MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setContent(emailContent,"text/html");
            parts.addBodyPart(bodyPart);
		
            if (CDIS.getProperty("rptVfcudirListFiles").equals("true") || (this.failedIdName.size() > 0)) { 
                //add the attachment
                File reportFile = new File (this.rptFile);
             
                MimeBodyPart attachmentPart = new MimeBodyPart();
                DataSource source = new FileDataSource(reportFile);
                attachmentPart.setDataHandler(new DataHandler(source));
                attachmentPart.setFileName(reportFile.getName());
                parts.addBodyPart(attachmentPart);
            }
            
            // add the Multipart to the message
            message.setContent(parts);
            Transport.send(message);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    /*  Method :       statisticsGenerate
        Arguments:      
        Description:   Generates list of statistics for the report
        RFeldman 2/2017
    */  
    private void statisticsGenerate () {
            
        if (completedStepSql.contains("'LCC'")) {
            if (completedIdName.size() > 0) {
                statsHeader = "\nNumber of Media Records Created/Integrated with CIS : " + this.completedIdName.size();
            }
            else {
                statsHeader = "\nNumber of Media Records Created/Integrated with CIS : 0" ;
            }
        }
        else if (completedStepSql.contains("'LDC'")) {
            if (completedIdName.size() > 0) {
                statsHeader = "\nNumber of Media Records Ingested/linked in DAMS : " + this.completedIdName.size();
            }
            else {
                statsHeader = "\nNumber of Media Records Ingested/linked in DAMS : 0";
            }
        }
            
        if (failedIdName.size() > 0) {
            statsHeader = statsHeader + "\nNumber of Failed Records: " + this.failedIdName.size();
        }
        else {
            statsHeader = statsHeader +  "\nNumber of Failed Records: 0";
        }
    }
    
    /*  Method :       writeCompleted
        Arguments:      
        Description:   Adds the list of the completed records to the physical report
        RFeldman 2/2017
    */  
    private void writeCompleted() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = null;
            
            if (completedStepSql.contains("'LCC'")) {
                sectionHeader = "\n\nThe Following Media Successfully Created/Integerated with CIS: ";
            }
            else if (completedStepSql.contains("'LDC'")) {
                sectionHeader = "\n\nThe Following Media Successfully Ingested / Linked with DAMS: ";
            }
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

            if (! (completedIdName.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "There have been no successful media";
                document.add(new Phrase("\n" + listing,listElementFont));
                return;
                
            }
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Integer mapId :completedIdName.keySet()) {     
            try {
                
                String listing = null;
                
                CDISMap cdisMap = new CDISMap();
                SiAssetMetaData siAsst = new SiAssetMetaData();
                
                cdisMap.setCdisMapId(mapId);
                cdisMap.populateMapInfo();
                
                siAsst.setUoiid(cdisMap.getDamsUoiid());
                siAsst.populateOwningUnitUniqueName();
                      
                RtfFont listElementFont=new RtfFont("Courier",8);
                
                if (completedIdName.get(mapId).equals("emuLinkComplete") ) {
                    listing = "File: " + cdisMap.getFileName() + " UAN: " + siAsst.getOwningUnitUniqueName() + " IRN: " + cdisMap.getCisUniqueMediaId() ;
                }
                else  {
                    listing = "File: " + cdisMap.getFileName() + " UAN: " + siAsst.getOwningUnitUniqueName();
                }
                                         
                document.add(new Phrase("\n" + listing,listElementFont));
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
                        
        }
        
    }
    
    /*  Method :       writeFailed
        Arguments:      
        Description:   Adds the list of the failed records to the physical report
        RFeldman 2/2017
    */  
    private void writeFailed() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = "\n\nThe Following Media Experienced Integration Failures: ";
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

             if (! (failedIdName.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "No Errors reported";
                document.add(new Phrase("\n" + listing,listElementFont));
                
                return;
            }
             
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Integer errorId : failedIdName.keySet()) {  
            
            try {
                
                CDISErrorLog cdisError = new CDISErrorLog();
                      
                RtfFont listElementFont=new RtfFont("Arial",10);
            
                cdisError.setCdisErrorId(errorId);
                cdisError.populateCdisMapId();
                
                String errorDescription = cdisError.returnDescription();
                
                String listing = "FileName: " + failedIdName.get(errorId) +  ",   Error: " + errorDescription ; 
                
                document.add(new Phrase("\n" + listing,listElementFont));
                
                
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }           
        }     
    }
}
