/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISError;
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

public class Report {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    private String rptHours;
    private LinkedHashMap<Integer, String> completedIdName;
    private LinkedHashMap<Integer, String> failedIdName;
    private Document document;
    private String rptFile;
    private String rptVendorDir;
    private Integer masterMd5Id;
    private Integer childMd5Id;
    
    private boolean create () {
        
        String timeStamp;
        String timeStampWords;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());

        this.rptFile =  "rpt\\CDISRPT-" + CDIS.getProperty("siHoldingUnit") + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            
            if ( CDIS.getProperty("rptType").equals("timeframe") ) {
                document.add(new Paragraph(timeStampWords + "\n" + 
                    CDIS.getProperty("siHoldingUnit") + " CDIS Activity Report- Past " + this.rptHours + " Hours", title));
            }
            else if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
                document.add(new Paragraph(timeStampWords + "\n" + 
                        CDIS.getProperty("siHoldingUnit") + " CDIS Activity Report- " + this.rptVendorDir, title));
            }
            else {
                logger.log(Level.FINEST, "Invalid report Type");
                return false;
            }

        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report ");
            return false;
        }  
        
        return true;
    }
    
    private boolean populateRptVendorDir() {
        String sql = "SELECT SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '\\', 1, 1)-1) " +
                     "FROM vfcu_md5_file " +
                     "WHERE vfcu_md5_file_id = " + this.masterMd5Id;
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                //Add the records to the masterMd5Id list 
                 this.rptVendorDir = rs.getString(1);
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
    
    private boolean populateMasterMd5FileId () {
        
        String sqlTypeArr[] = null;
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
                if (sqlTypeArr[0].equals("getMasterMd5Id")) {   
                    sql = key;    
            }
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                //Add the records to the masterMd5Id list 
                 this.masterMd5Id = rs.getInt(1);
            }        
            else {
                throw new Exception();
            }
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for report, returning", e);
                return false;
	}
        
        return true;
    }
    
    private boolean populateChildMd5FileId () {
        
        String sql = "SELECT vfcu_md5_file_id " +
                    "FROM vfcu_md5_file " +
                    "WHERE master_md5_file_id = " + this.masterMd5Id +
                    " AND master_md5_file_id != vfcu_md5_file_id ";
                
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                //Add the records to the masterMd5Id list 
                 this.childMd5Id = rs.getInt(1);
            }        
            else {
                throw new Exception();
            }
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for report, returning", e);
                return false;
	}
        
        return true;
    }
    
    int countPendingRecords () {
        int numPendingRecords = 0;
        
        String sql = "SELECT COUNT(*) " + 
                     "FROM   vfcu_media_file a " +
                     "WHERE  a.vfcu_md5_file_id in (" + this.masterMd5Id + ", " + this.childMd5Id + ")" +
                     " AND NOT EXISTS ( " +
                     "      SELECT  'X' " + 
                     "FROM    vfcu_activity_log b " +
                     "WHERE   a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "AND     b.vfcu_status_cd in ('PS','ER')) ";
        
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
        
        if ( CDIS.getProperty("rptType").equals("timeframe") ) {
        
            try {
                this.rptHours = CDIS.getProperty("rptHours");
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "Unable to calculate timeframe of report, defaulting to last 24 hours");
                this.rptHours = "24";
            }
        } 
        else if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
            //get masterMd5FileId to run report for (FROM XML)
            boolean md5FileIdFound = populateMasterMd5FileId();
   
            if (! md5FileIdFound) {
                logger.log(Level.FINEST, "No md5 file obtained, or none to report meeting condition...cannot generate report.");
                return;
            }
        
            //get childMd5FileId for the master XML
            md5FileIdFound = populateChildMd5FileId ();
            if (! md5FileIdFound) {
                logger.log(Level.FINEST, "No child md5 file obtained, or none to report meeting condition...cannot generate report.");
                return;
            }
        
            //check for completion/errors (counts) of md5 file
            int numPendingRecords = countPendingRecords();
        
            if (! (numPendingRecords == 0)) {
                logger.log(Level.FINEST, "Some records are in pending state...holding off for report generation.");
                return;
            }
        
            //get the vendordir name for the report
            populateRptVendorDir();
        }
        
        create();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.completedIdName = new LinkedHashMap<>();
        genCompletedIdList ();
     
        //Get the failed records from the past increment
        this.failedIdName = new LinkedHashMap<>();
        genFailedIdList ();
        
        statisticsWrite();
                
        if (! CDIS.getProperty("rptStatsOnly").equals("true") ) {
            //failed list is to be displayed before successful list per Ken
            writeFailed();
                
            //now write successful completion list to file
            writeCompleted();

        }
        
        //close the Document
        document.close();
        
        if (CDIS.getProperty("emailReportTo") != null) { 
            //send email to list
            logger.log(Level.FINEST, "Need To send Email Report");
            
            send();
        }
        if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
            //set the report generated flag to indicate report was generated
            VFCUMd5File vfcuMd5File = new VFCUMd5File();
            vfcuMd5File.setVfcuMd5FileId(this.masterMd5Id);
            vfcuMd5File.updateCdisRptDt();
        
            vfcuMd5File.setVfcuMd5FileId(this.childMd5Id);
            vfcuMd5File.updateCdisRptDt();
        
            try { if ( CDIS.getDamsConn()!= null)  CDIS.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        }
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
        
        if ( CDIS.getProperty("rptType").equals("timeframe") ) {
            if (sql.contains("?RPT_HOURS?")) {
                sql = sql.replace("?RPT_HOURS?", this.rptHours);
            }
        }
        else if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
            if (sql.contains("?MD5_MASTER_ID?")) {
                sql = sql.replace("?MD5_MASTER_ID?", Integer.toString(this.masterMd5Id)) ;
            }
            if (sql.contains("?MD5_CHILD_ID?")) {
                sql = sql.replace("?MD5_CHILD_ID?", Integer.toString(this.childMd5Id));
            }
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
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
                if (sqlTypeArr[0].equals("getSuccessRecords")) {   
                    sql = key;    
            }
        }
        
        if ( CDIS.getProperty("rptType").equals("timeframe") ) {
            if (sql.contains("?RPT_HOURS?")) {
                sql = sql.replace("?RPT_HOURS?", this.rptHours);
            }
        }
        else if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
            if (sql.contains("?MD5_MASTER_ID?")) {
                sql = sql.replace("?MD5_MASTER_ID?", Integer.toString(this.masterMd5Id)) ;
            }
            if (sql.contains("?MD5_CHILD_ID?")) {
                sql = sql.replace("?MD5_CHILD_ID?", Integer.toString(this.childMd5Id));
            }
        }
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(sql);
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
            String[] toEmailAddrArray = CDIS.getProperty("emailReportTo").split(",");
            for (int i = 0; i < toEmailAddrArray.length; i++) {
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
            }
	
            String emailContent = null;
            
            if ( CDIS.getProperty("rptType").equals("timeframe") ) {
                message.setSubject(CDIS.getProperty("siHoldingUnit") + ": CDIS Activity Report - Past " + this.rptHours + " Hours" ); 
            }
            else if ( CDIS.getProperty("rptType").equals("vfcuDir") ) {
                message.setSubject(CDIS.getProperty("siHoldingUnit") + ": CDIS Activity Report - " + this.rptVendorDir);
            }
            
            emailContent = "<br>Please see the attached CDIS Activity Report<br><br><br><br>" +
                    "If you have any questions regarding information contained in this report, please contact: <br>" + 
                    "Robert Feldman (FeldmanR@si.edu) or Isabel Meyer (MeyerI@si.edu) <br><br><br><br>" + 
                    "Please let us know if someone else in your organization requires this information, or if you would like for us to discontinue delivery of this report to you.<br>" +
                    "Thanks! The DAMS team";
            
            // create the Multipart and its parts to it
            Multipart parts = new MimeMultipart();

            // create and fill the Body
           
            MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setContent(emailContent,"text/html");
            parts.addBodyPart(bodyPart);
		
            //add the attachment
            File reportFile = new File (this.rptFile);
             
            MimeBodyPart attachmentPart = new MimeBodyPart();
	    DataSource source = new FileDataSource(reportFile);
	    attachmentPart.setDataHandler(new DataHandler(source));
	    attachmentPart.setFileName(reportFile.getName());
	    parts.addBodyPart(attachmentPart);

            // add the Multipart to the message
            message.setContent(parts);
            Transport.send(message);
            
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    
    private void statisticsWrite () {
        
        try {
            RtfFont stats=new RtfFont("Times New Roman",12);
            
            if (completedIdName.size() > 0) {
                document.add(new Paragraph("\nNumber of Succesful Integrations Records : " + this.completedIdName.size(), stats));
            }
            else {
                document.add(new Paragraph("\nNumber of Succesful Integrations Records : 0", stats));
            }
            
            if (failedIdName.size() > 0) {
                document.add(new Paragraph("Number of Failed Records: " + this.failedIdName.size(), stats));
            }
            else {
                document.add(new Paragraph("Number of Failed Records: 0", stats));
            }

            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
    }
    
    
    private void writeCompleted() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = null;
            
            sectionHeader = "\n\nThe Following Media Successfully Processed: ";
                          
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

            if (! (completedIdName.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "There have been no successful media copies";
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
                    listing = "File: " + cdisMap.getFileName() + " Linked UAN: " + siAsst.getOwningUnitUniqueName() + " IRN: " + cdisMap.getCisUniqueMediaId() ;
                }
                else if (completedIdName.get(mapId).equals("DAMSIngestComplete") ) {
                    listing = "File: " + cdisMap.getFileName() + " Ingested UAN: " + siAsst.getOwningUnitUniqueName();
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
                
                CDISError cdisError = new CDISError();
                      
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
