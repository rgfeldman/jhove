package edu.si.damsTools.vfcu;

import edu.si.damsTools.vfcu.database.VfcuErrorLog;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;

import com.lowagie.text.Font;
import com.lowagie.text.Document;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Phrase;
import com.lowagie.text.rtf.RtfWriter2;
import com.lowagie.text.rtf.style.RtfFont;
import edu.si.damsTools.DamsTools;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.ArrayList;
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
import edu.si.damsTools.cdis.operations.Operation;
import edu.si.damsTools.utilities.XmlQueryData;
import edu.si.damsTools.vfcu.database.VfcuMd5FileHierarchy;
import edu.si.damsTools.vfcu.database.VfcuMd5FileActivityLog;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

public class Report extends Operation {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private Integer currentMd5FileId;
    private ArrayList<Integer> masterMd5Ids;
    private Integer childMd5Id;
    private LinkedHashMap<Integer, String> completedIdName;
    private ArrayList<Integer> failedIdList;  //Error IDs rather than media records, there can be failures that dont have media record
    private Document document;
    private String rptFile;
    private String rptShrtVendorDir;
    private String rptFullVendorDir;
    private String statsHeader;
    
    public Report() {
    }
    
    private boolean create () {
        
        String timeStamp;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        this.rptFile =  DamsTools.getDirectoryName()+ "/rpt/VFCURPT-" + DamsTools.getProjectCd().toUpperCase() + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",12,Font.BOLD);
            
            document.add(new Paragraph(DamsTools.getProjectCd().toUpperCase() + " VFCU Activity Report \nDirectory: " + this.rptFullVendorDir, title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report", e);
        }  
        
        return true;
    }
    
    private boolean populateVendorDirs() {
        String sql = null;
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
             sql = "SELECT base_path_vendor, SUBSTR (file_path_ending, 1, INSTR(file_path_ending, '/', -1) -1 ) " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + this.currentMd5FileId;
        } 
        else {
            sql = "SELECT base_path_vendor, file_path_ending " +
                    "FROM vfcu_md5_file " +
                    "WHERE vfcu_md5_file_id = " + this.currentMd5FileId;
        }
           
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            if (rs.next()) {
                //Add the records to the masterMd5Id list 
                String strBasePathVendor = rs.getString(1);
                String strFilePathEnding = rs.getString(2);
                  
                if (strFilePathEnding == null) {
                    this.rptShrtVendorDir = strBasePathVendor.substring(strBasePathVendor.lastIndexOf('/') + 1);
                    this.rptFullVendorDir = strBasePathVendor;
                }
                else {
                    this.rptShrtVendorDir = strFilePathEnding;
                    this.rptFullVendorDir = strBasePathVendor + "/" + this.rptShrtVendorDir;
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
    
    public void invoke () {
        
        masterMd5Ids = new ArrayList<> () ;
        
        //get masterMd5FileId to run report for (FROM XML)
        populateMasterMd5FileIds();
   
        if (masterMd5Ids.isEmpty() ) {
            logger.log(Level.FINEST, "No md5 file obtained, or none to report meeting condition...cannot generate report.");
            return;
        }
        
        // NEED TO LOOP THROUGH MD5 IDs one at a time
        for (Integer masterMd5Id : masterMd5Ids) {
            
            VfcuMd5File vfcuMd5File = new VfcuMd5File();
            vfcuMd5File.setVfcuMd5FileId(masterMd5Id);
            vfcuMd5File.populateBasicDbData();
            
            //vfcuMd5File.setMasterMd5FileId(masterMd5Id);
            currentMd5FileId = masterMd5Id;
            this.childMd5Id = null;
            
            if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            //get childMd5FileId for the master XML
            
                VfcuMd5FileHierarchy vfcuMd5FileHierarchy = new  VfcuMd5FileHierarchy();
                vfcuMd5FileHierarchy.setMasterFileVfcuMd5FileId(masterMd5Id);
                vfcuMd5FileHierarchy.populateSubfileIdForMasterId();
                
                this.childMd5Id = vfcuMd5FileHierarchy.getSubFileVfcuMd5FileId();
                if (this.childMd5Id == null) {
                    logger.log(Level.FINEST, "No child md5 file obtained, or none to report meeting condition...cannot generate report.");
                    continue;
                }
            }
            
            //get the vendordir name for the report
            populateVendorDirs();
        
            //first get the completed list
            this.completedIdName = new LinkedHashMap<>();
            genCompletedIdList ();
     
            //Get the failed records from the past increment
            this.failedIdList = new ArrayList<>();
            genFailedIdList ();
            
            //if both master and child md5 file are complete, generate the detailed lists.  
            if (completedIdName.size() + failedIdList.size() == 0 ) {
                //no report to generate, do not sending anything, or even create the report
                continue;
            }
            
            boolean reportStarted = create();
                if (!reportStarted) {
                logger.log(Level.FINEST, "Report Creation Failed");
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
        
            if (failedIdList.size() > 0) {
                //failed list is to be displayed before successful list per Ken
                writeFailed();
            }
                
            //now write successful completion list to file
            if (completedIdName.size() > 0) {
                writeCompleted();
            }

            //close the Document
            document.close();
        
        
            if (DamsTools.getProperty("emailReportTo") != null) { 
                //send email to list
                logger.log(Level.FINEST, "Need To send Email Report");
            
                send ();
            }
        
            //set the report generated flag to indicate report was generated
            VfcuMd5FileActivityLog vfcuMd5FileActivityLog = new VfcuMd5FileActivityLog();
            vfcuMd5FileActivityLog.setVfcuMd5FileId(currentMd5FileId);
            vfcuMd5FileActivityLog.setVfcuMd5StatusCd("RE");
            vfcuMd5FileActivityLog.insertRecord();
            
            if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
                vfcuMd5FileActivityLog = new VfcuMd5FileActivityLog();
                vfcuMd5FileActivityLog.setVfcuMd5FileId(this.childMd5Id);
                vfcuMd5FileActivityLog.setVfcuMd5StatusCd("RE");
                vfcuMd5FileActivityLog.insertRecord();
            }
        }
         
        try { if ( DamsTools.getDamsConn()!= null)  DamsTools.getDamsConn().commit(); } catch (Exception e) { e.printStackTrace(); }
        
    }
    
    
    private boolean populateMasterMd5FileIds () {
        
        String sql = null;
        for(XmlQueryData xmlInfo : DamsTools.getSqlQueryObjList()) {
            sql = xmlInfo.getDataForAttribute("type","getMasterMd5Ids");
            if (sql != null) {
                break;
            }
        }
        if (sql == null) {
            logger.log(Level.SEVERE, "Error: Required sql not found");
            return false;
        }
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
            ResultSet rs = stmt.executeQuery() ) {

            //Add the value from the database to the list
            while (rs.next()) {
                if (rs.getInt(1) > 0) {
                    masterMd5Ids.add(rs.getInt(1));
                }
            }
        }
        catch(Exception e) {
            logger.log(Level.SEVERE, "Error obtaining list to sync mediaPath and Name", e);
            return false;
        }
        return true; 
    }
    
    
    int countPendingRecords () {
        int numPendingRecords = 0;
        
        String statusToCheck = null;
        String md5FileIdsToQuery = null;
        
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            statusToCheck = "PS"; 
            md5FileIdsToQuery = this.currentMd5FileId + ", " + this.childMd5Id;
        }
        else {
            statusToCheck = "PM";
            md5FileIdsToQuery = this.currentMd5FileId.toString();
        }
        
        
        String sql = "SELECT COUNT(*) " + 
                     "FROM   vfcu_media_file a " +
                     "WHERE  a.vfcu_md5_file_id in (" + md5FileIdsToQuery + ")" +
                     " AND NOT EXISTS ( " +
                     "      SELECT  'X' " + 
                     "FROM    vfcu_activity_log b " +
                     "WHERE   a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "AND     b.vfcu_status_cd in ('" + statusToCheck + "','ER')) ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

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
    
    private boolean genFailedIdList () {
        
        String md5FileIdsToQuery = null;
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            md5FileIdsToQuery = this.currentMd5FileId + ", " + this.childMd5Id;
        } else {
            md5FileIdsToQuery = this.currentMd5FileId.toString();
        }
        String sql = "SELECT vfcu_error_log_id " +
                     "FROM vfcu_error_log " +
                     "WHERE vfcu_md5_file_id in (" + md5FileIdsToQuery + ") " +
                     "ORDER BY file_name ";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                this.failedIdList.add(rs.getInt(1));
            }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for failed report, returning", e);
                return false;
	}
        
        return true;
    }
    
    private boolean genCompletedIdList () {
        
        String statusToCheck = null;
         String md5FileIdsToQuery = null;
         
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            statusToCheck = "PS"; 
            md5FileIdsToQuery = this.currentMd5FileId + ", " + this.childMd5Id;
        }
        else {
            statusToCheck = "PM"; 
            md5FileIdsToQuery = this.currentMd5FileId.toString();
        }
        
        String sql = "SELECT a.vfcu_media_file_id, media_file_name " +
                     "FROM vfcu_media_file a, " +
                     "      vfcu_activity_log b " +
                     "WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "AND   vfcu_md5_file_id IN (" + md5FileIdsToQuery + ") " +
                     "AND b.vfcu_status_cd = '" + statusToCheck + "' " +
                     "AND NOT EXISTS ( " +
                     " SELECT 'X' " +
                     " FROM  vfcu_error_log c " +
                     " WHERE a.vfcu_media_file_id = c.vfcu_media_file_id )" +
                     "ORDER BY media_file_name "; 
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement stmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {
                this.completedIdName.put(rs.getInt(1), rs.getString(2));
            }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for completed report, returning", e);
                return false;
	}
        
        return true;
    }
    
    private String genEmailBody() {
        
        String emailText = "<br><br><br>Please see the attachment for detailed list of files<br>" +
            "If you have any questions regarding information contained in this report, please contact: <br>" + 
            "Robert Feldman (FeldmanR@si.edu) or Isabel Meyer (MeyerI@si.edu) <br><br><br>" + 
            "Please let us know if someone else in your organization requires this information, or if you would like for us to discontinue delivery of this report to you.<br>" +
            "Thanks! The DAMS team";
        
        String emailContent = "Source Directory: " + this.rptFullVendorDir + "<br>" +  this.statsHeader.replace("\n","<br>") + emailText;
    
        return emailContent;
        
    }
    
    public void send () {
        try {
            Properties mailProps = new Properties();
            String server = "smtp.si.edu";
            mailProps.put("mail.smtp.host", server);      
            Session session = Session.getDefaultInstance( mailProps, null );

            MimeMessage message = new MimeMessage( session );
            message.setFrom(new InternetAddress("no-reply@dams.si.edu"));
            String[] toEmailAddrArray = DamsTools.getProperty("emailReportTo").split(",");
            for (String toEmailAddr : toEmailAddrArray) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddr.trim()));
            }
            
            message.setSubject(DamsTools.getProjectCd().toUpperCase() + ": VFCU Activity Report - " + this.rptShrtVendorDir);  
         
            // Generate appropriate text for email body
            String emailContent = genEmailBody();
            
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
            logger.log(Level.FINEST, "Error in file delivery: ", e);
        }
        
    }
    
    
    private void statisticsGenerate () {
        
        if (completedIdName.size() > 0) {
            statsHeader = "\nNumber of Successful Media File Validation/Transfers : " + this.completedIdName.size();
        }
        else {
            //document.add(new Paragraph("\nNumber of Sucessful Media File Validation/Transfers : 0", stats));
            statsHeader = "\nNumber of Successful Media File Validation/Transfers : 0";
        }
            
        if (failedIdList.size() > 0) {
            statsHeader = statsHeader + "\nNumber of Failed Records : " + this.failedIdList.size();
        }
        else {
            statsHeader = statsHeader + "\nNumber of Failed Records : 0";
        }
    }
    
    private void writeCompleted() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = "\n\nThe Following Media Successfully Copied, Validated and Recorded: ";
                          
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
        
        for (Integer mediaId :completedIdName.keySet()) {     
            try {
                
                VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
                      
                RtfFont listElementFont=new RtfFont("Courier",8);
                
                vfcuMediaFile.setVfcuMediaFileId(mediaId);
                vfcuMediaFile.setMediaFileName(completedIdName.get(mediaId));
                vfcuMediaFile.populateMediaFileAttr();
               
                String listing = "File: " + vfcuMediaFile.getMediaFileName() +
                            " Dated: " + vfcuMediaFile.getMediaFileDate() ;
                                         
                document.add(new Phrase("\n" + listing,listElementFont));
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }
        
    }
    
    
    private void writeFailed() {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);

            String sectionHeader = "\n\nThe Following Media Experienced Failures: ";
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

             if (! (failedIdList.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "No Errors reported";
                document.add(new Phrase("\n" + listing,listElementFont));
                
                return;
            }
             
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Integer vfcuErrorLogId : failedIdList) {  
            
            try {
                
                RtfFont listElementFont=new RtfFont("Courier",8);
            
                VfcuErrorLog vfcuError = new VfcuErrorLog();
                vfcuError.setVfcuErrorLogId(vfcuErrorLogId);
                vfcuError.populateDescriptiveInfo();

                String listing = "File: " + vfcuError.getFileName() +  "   Error: " + vfcuError.returnErrDescriptionForErrorCd()  + " " +
                        vfcuError.getAddlErrorInfo(); 
    
                document.add(new Phrase("\n" + listing,listElementFont));
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }     
    }
    
    public ArrayList<String> returnRequiredProps () {
        
        ArrayList<String> reqProps = new ArrayList<>();
        reqProps.add("useMasterSubPairs");
        //add more required props here
        return reqProps;    
    }
}