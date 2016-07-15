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
   
    private Integer currentMasterMd5FileId;
    private Integer childMd5Id;
    private LinkedHashMap<Integer, String> completedIdName;
    private String completedStepSql;
    private Document document;
    private ArrayList<Integer> masterMd5Ids;
    private LinkedHashMap<Integer, String> failedIdName;
    private String rptFile;
    private String rptVendorDir;
    private String statsHeader;
    
    private boolean create () {
        
        String timeStamp;
        String timeStampWords;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());

        this.rptFile =  CDIS.getCollectionGroup() + "\\rpt\\CDISRPT-" + CDIS.getCollectionGroup() + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            
            document.add(new Paragraph(timeStampWords + "\n" + 
                CDIS.getCollectionGroup()+ " CDIS Activity Report- " + this.rptVendorDir, title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report ");
            return false;
        }  
        
        return true;
    }
    
    private boolean populateRptVendorDir() {
        
        String sql = null;
        
        if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
        sql = "SELECT SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '\\', 1, 1)-1) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + this.currentMasterMd5FileId;
        } 
        else {
            sql = "SELECT file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + this.currentMasterMd5FileId;
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
    
    private boolean populateMasterMd5FileIds () {
        
        String sqlTypeArr[] = null;
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
                if (sqlTypeArr[0].equals("getMasterMd5Ids")) {   
                    sql = key;    
            }
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                //Add the records to the masterMd5Id list 
                 masterMd5Ids.add(rs.getInt(1));
            }       
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for report, returning", e);
                return false;
	}
        
        return true;
    }
    
    int countPendingRecordsLDC () {
        int numPendingRecords = 0;
        
        String queryParam = null;
        if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            queryParam = "WHERE  a.vfcu_md5_file_id in (" + this.currentMasterMd5FileId + ", " + this.childMd5Id + ") ";
        }
        else {
            queryParam = "WHERE  a.vfcu_md5_file_id = " + this.currentMasterMd5FileId;       
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
                     "  AND   c.cdis_status_cd in ('" + CDIS.getProperty("rptStatus") + "','ERR')) " +
                     "AND  NOT EXISTS ( " +  
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
    
    int countPendingRecordsLCC () {
        int numPendingRecords = 0;
        
        String sql = "SELECT COUNT(*) " +
                     "FROM   vfcu_media_file a " +
                     "WHERE  a.vfcu_md5_file_id in (" + this.childMd5Id + ") " +
                     "AND  NOT EXISTS ( " +
                     "  SELECT 'X' " +
                     "  FROM   cdis_map b, " +
                     "         cdis_activity_log c " +
                     "  WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "  AND b.cdis_map_id = c.cdis_map_id " +
                     "  AND   c.cdis_status_cd in ('LCC','ERR')) " +
                     "AND  NOT EXISTS ( " +  
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
    
    
    public void generate () {
        
        VFCUMd5File vfcuMd5File = new VFCUMd5File();
        masterMd5Ids = new ArrayList<> () ;
        
        //get masterMd5FileId to run report for (FROM XML)
        populateMasterMd5FileIds();
   
        if (masterMd5Ids.isEmpty() ) {
            logger.log(Level.FINEST, "No md5 file obtained, or none to report meeting condition...cannot generate report.");
            return;
        }
        
        // NEED TO LOOP THROUGH MD5 IDs one at a time
        for (Integer masterMd5Id : masterMd5Ids) {
            
            vfcuMd5File.setMasterMd5FileId(masterMd5Id);
            currentMasterMd5FileId = masterMd5Id;
            this.childMd5Id = null;
            
            if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
            //get childMd5FileId for the master XML
            
                this.childMd5Id = vfcuMd5File.returnSubFileMd5Id(childMd5Id);
                if (this.childMd5Id == null) {
                    logger.log(Level.FINEST, "No child md5 file obtained, or none to report meeting condition...cannot generate report.");
                    continue;
                }
            }
        
            int numPendingRecords = 0;
        
            if (CDIS.getProperty("rptStatus").equals("LCC")) {
                //check for completion/errors (counts) of md5 file
                numPendingRecords = countPendingRecordsLCC();
            }
            else {
                numPendingRecords = countPendingRecordsLDC();
            }
        
            if (! (numPendingRecords == 0)) {
                logger.log(Level.FINEST, "Some records are in pending state...holding off for report generation.");
                continue;
            }
        
            //get the vendordir name for the report
            populateRptVendorDir();
                
            //if both master and child md5 file are complete, generate the detailed lists. 
            create();
        
            //Get list of completed records (UOI_IDs) from the past increment
            this.completedIdName = new LinkedHashMap<>();
            genCompletedIdList ();
     
            //Get the failed records from the past increment
            this.failedIdName = new LinkedHashMap<>();
            genFailedIdList ();
        
            //only continue if the completedList and FailedList have reoords
            if (this.completedIdName.isEmpty() && this.failedIdName.isEmpty() ) {
                logger.log(Level.FINEST, "No Rows to report, exiting");
                return;
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
        
            //set the report generated flag to indicate report was generated
            vfcuMd5File.setVfcuMd5FileId(this.currentMasterMd5FileId);
            vfcuMd5File.updateCdisRptDt();
        
            if (CDIS.getProperty("useMasterSubPairs").equals("true")) {
                vfcuMd5File.setVfcuMd5FileId(this.childMd5Id);
                vfcuMd5File.updateCdisRptDt();
            }
        }
        
        try { if ( CDIS.getDamsConn()!= null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
    }
   
    
    private boolean genFailedIdList () {
        
        String sqlTypeArr[] = null;
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
                if (sqlTypeArr[0].equals("getFailedRecords")) {   
                    sql = key;    
            }
        }
        
        if (sql.contains("?MD5_MASTER_ID?")) {
            sql = sql.replace("?MD5_MASTER_ID?", Integer.toString(this.currentMasterMd5FileId)) ;
        }
        if (sql.contains("?MD5_CHILD_ID?")) {
            sql = sql.replace("?MD5_CHILD_ID?", Integer.toString(this.childMd5Id));
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                this.failedIdName.put(rs.getInt(1), rs.getString(2));    
            }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
                return false;
	}
        
        return true;
    }
    
    private boolean genCompletedIdList () {
        
        String sqlTypeArr[] = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);   
            
            if (sqlTypeArr[0].equals("getSuccessRecords")) {   
                this.completedStepSql = key;   
            }
        }
       
        if (completedStepSql.contains("?MD5_MASTER_ID?")) {
            completedStepSql = completedStepSql.replace("?MD5_MASTER_ID?", Integer.toString(this.currentMasterMd5FileId)) ;
        }
        if (completedStepSql.contains("?MD5_CHILD_ID?")) {
            completedStepSql = completedStepSql.replace("?MD5_CHILD_ID?", Integer.toString(this.childMd5Id));
        }
        
        logger.log(Level.FINEST, "SQL: {0}", completedStepSql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(completedStepSql);
             ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                this.completedIdName.put(rs.getInt(1), rs.getString(2));    
            }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for report, returning", e);
                return false;
	}
        
        return true;
    }
    
    
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
                message.setSubject(CDIS.getCollectionGroup()+ ": Batch Hot Folder Import Activity Report - " + this.rptVendorDir);
            }
            else {
                message.setSubject(CDIS.getCollectionGroup()+ ": Batch Hot Folder Integration Activity Report - " + this.rptVendorDir);
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
    
    
    private void statisticsGenerate () {
            
        if (completedStepSql.contains("'LCC'")) {
            if (completedIdName.size() > 0) {
                statsHeader = "\nNumber of Media Records Integrated with CIS : " + this.completedIdName.size();
            }
            else {
                statsHeader = "\nNumber of Media Records Integrated with CIS : 0" ;
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
    
    private void writeCompleted() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = null;
            
            if (completedStepSql.contains("'LCC'")) {
                sectionHeader = "\n\nThe Following Media Successfully Integerated with CIS: ";
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
