/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISObjectMap;
import edu.si.CDIS.Database.CDISErrorLog;
import edu.si.CDIS.CIS.Database.Objects;
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
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Date;
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

public class GenTimeframeReport {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
    private String rptHours;
    private LinkedHashMap <Integer, String> failedIdName;  
    private ArrayList <Integer> ldcMapIds;  
    private ArrayList <Integer> mdsMapIds;
    private ArrayList <Integer> lccMapIds;
    private Document document;
    private String rptFile;
    private String completedStepSql;
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
            
            document.add(new Paragraph(timeStampWords + "\n" + 
                    CDIS.getProperty("siHoldingUnit") + " CDIS Activity Report- Past " + this.rptHours + " Hours", title));

        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR, cannot create report ");
            return false;
        }  
        
        return true;
    }
    
    int countPendingRecords () {
        int numPendingRecords = 0;
        
        String sql = "SELECT COUNT(*) " +
                     "FROM   vfcu_media_file a " +
                     "WHERE  a.vfcu_md5_file_id in (" + this.masterMd5Id + ", " + this.childMd5Id + ") " +
                     "AND  NOT EXISTS ( " +
                     "    SELECT 'X' " +
                     "    FROM   cdis_map b, " +
                     "           cdis_activity_log c " +
                     "    WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                     "    AND   c.cdis_status_cd in ('LDC','ER'))";
        
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
        
        this.rptHours = CDIS.getProperty("rptHours");
                
        create();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.lccMapIds = new ArrayList();
        this.ldcMapIds = new ArrayList();
        this.mdsMapIds = new ArrayList();
         
        genStepCompletedList ();   
        
        //Get the failed records from the past increment
        this.failedIdName = new LinkedHashMap<>();
        genFailedIdList ();
        
        statisticsWrite();
                
        if (! CDIS.getProperty("rptTimeframeStatsOnly").equals("true") ) {
            //failed list is to be displayed before successful list per Ken
            writeFailed();
                
            if (completedStepSql.contains("'LCC'")) {
                //now write successful completion list to file
                writeStepCompleted(lccMapIds, "LCC");
            }
            
            if (completedStepSql.contains("'LDC'")) {
                //now write successful completion list to file
                writeStepCompleted(ldcMapIds, "LDC");
            }
            
            if (completedStepSql.contains("'MDS'")) {
                //now write successful completion list to file
                writeStepCompleted(mdsMapIds, "MDS");
            }
            
            
        }
        
        //close the Document
        document.close();
        
        if (CDIS.getProperty("timeFrameEmailList") != null) { 
            //send email to list
            logger.log(Level.FINEST, "Need To send Email Report");
            
            send();
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
        
        if (sql.contains("?RPT_HOURS?")) {
            sql = sql.replace("?RPT_HOURS?", this.rptHours);
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
    
    
    private boolean genStepCompletedList () {
        
        String sqlTypeArr[] = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
            if (sqlTypeArr[0].equals("getStepCompleteRecords")) {   
                completedStepSql = key;  
            }        
        }
        
        if (completedStepSql.contains("?RPT_HOURS?")) {
            completedStepSql = completedStepSql.replace("?RPT_HOURS?", this.rptHours);
        }
        
        logger.log(Level.FINEST, "SQL: {0}", completedStepSql);
        try (PreparedStatement stmt = CDIS.getDamsConn().prepareStatement(completedStepSql);
             ResultSet rs = stmt.executeQuery() ) {

            while (rs.next()) {

                switch (rs.getString(2)) {
                    case "LCC":
                        this.lccMapIds.add(rs.getInt(1));
                        break;
                    case "LDC" :
                        this.ldcMapIds.add(rs.getInt(1));
                        break;    
                    case "MDS" :
                        this.mdsMapIds.add(rs.getInt(1));
                        break;   
                    default :
                        logger.log(Level.SEVERE, "Error: Encountered status type that is not reported");
                }  
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
            String[] toEmailAddrArray = CDIS.getProperty("timeFrameEmailList").split(",");
            for (int i = 0; i < toEmailAddrArray.length; i++) {
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
            }
	
            String emailContent = null;
            
            message.setSubject(CDIS.getProperty("siHoldingUnit") + ": CDIS 3.0 Activity Report - Past " + this.rptHours + " Hours" ); 
            
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
            
            if (this.failedIdName.size() > 0) {
                document.add(new Paragraph("\nNumber of Failed Records: " + this.failedIdName.size(), stats));
            }
            else {
                document.add(new Paragraph("\nNumber of Failed Records: 0", stats));
            }
                        
            // Only put out the header information for applicable statuses.
            // We check because sometimes we have zero rows for statuses we are looking for, but we would want to put on the report zero rows
            if (completedStepSql.contains("'LCC'")) {
                if (this.lccMapIds.size() > 0) {
                    document.add(new Paragraph("Number of media records linked back to the CIS : " + this.lccMapIds.size(), stats));
                }
                else {
                    document.add(new Paragraph("Number of media records linked back to the CIS : 0", stats));
                }
            }
            
            if (completedStepSql.contains("'LDC'")) {
                if (this.ldcMapIds.size() > 0) {
                    document.add(new Paragraph("Number of media records linked to DAMS: " + this.ldcMapIds.size(), stats));
                }
                else {
                    document.add(new Paragraph("Number of media records linked to DAMS: 0", stats));
                }
            }
            
            if (completedStepSql.contains("'MDS'")) {
                if (this.mdsMapIds.size() > 0) {
                    document.add(new Paragraph("Number of media records metadata synced: " + this.mdsMapIds.size(), stats));
                }
                else {
                    document.add(new Paragraph("Number of media records metadata synced: 0", stats));
                }
            }    
            

            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
    }
    
    private void writeStepCompleted(ArrayList<Integer> completedStep, String stepType) {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = null;
            
            switch (stepType) {
                case "LCC" :
                    sectionHeader = "\n\nThe Following Media has been linked back to the CIS: ";
                    break;
                case "LDC" :
                    sectionHeader = "\n\nThe Following Media has been linked to DAMS: ";
                    break;
                 case "MDS" :
                    sectionHeader = "\n\nThe Following Media has been metadata synced: ";
                    break;
                default:
                    logger.log(Level.FINEST, "Invalid stepType");
            }
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

            if (! (completedStep.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "There are no records during this timeframe period";
                document.add(new Phrase("\n" + listing,listElementFont));
                return;
                
            }
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Integer mapId : completedStep ) {     
            try {
                
                String listing = null;
                
                CDISMap cdisMap = new CDISMap();
                SiAssetMetaData siAsst = new SiAssetMetaData();
                CDISObjectMap cdisObjectMap = new CDISObjectMap();
                Objects objects = new Objects();
                
                cdisMap.setCdisMapId(mapId);
                cdisMap.populateMapInfo();
                
                cdisObjectMap.setCdisMapId(mapId);
                cdisObjectMap.populateCisUniqueObjectIdforCdisId();
                
                objects.setObjectID(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
                objects.populateObjectNumberForObjectID();
                
                siAsst.setUoiid(cdisMap.getDamsUoiid());
                siAsst.populateOwningUnitUniqueName();
                      
                RtfFont listElementFont=new RtfFont("Courier",8);
                
                switch (stepType) {
                    case "LCC" :
                         listing = "UAN: " + siAsst.getOwningUnitUniqueName() + " Linked To Object: " + objects.getObjectNumber() ;
                         break;
                    case "LDC" :
                         listing = "File: " + cdisMap.getFileName() + " linked to DAMS UAN: " + siAsst.getOwningUnitUniqueName();
                         break;
                    case "MDS" :   
                        listing = "UAN: " + siAsst.getOwningUnitUniqueName() + " Synced with object: " + objects.getObjectNumber();
                        break;
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
            
            String sectionHeader = null;
            
            sectionHeader = "\n\nThe Following Media has failed: ";
  
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

            if (! (this.failedIdName.size() > 0)) {
                RtfFont listElementFont=new RtfFont("Courier",8);
                String listing = "There are no Failed records during this timeframe period";
                document.add(new Phrase("\n" + listing,listElementFont));
                return;
                
            }
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Integer errorId : failedIdName.keySet()) {    
            try {
                
                String listing = null;
                
                CDISErrorLog cdisErrorLog = new CDISErrorLog();
                
                cdisErrorLog.setCdisErrorId(errorId);
                String errorDescription = cdisErrorLog.returnDescription();
                      
                RtfFont listElementFont=new RtfFont("Courier",8);
                
                listing = "File: " + failedIdName.get(errorId) + " Error: " + errorDescription;
                                          
                document.add(new Phrase("\n" + listing,listElementFont));
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
                        
        }
        
    }
    
}
