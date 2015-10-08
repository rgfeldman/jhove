/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import com.lowagie.text.*;
import com.lowagie.text.rtf.RtfWriter2;
import com.lowagie.text.rtf.style.RtfFont;

import edu.si.CDIS.DAMS.Database.CDISMap;
import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.DAMS.Database.CDISErrorCodeR;


public class Report {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Connection damsConn;
    private Double rptDays;
    private String rptHours;
    ArrayList<Integer> completedIds;
    ArrayList<Integer> metaSyncedIds;
    ArrayList<Integer> inProgressIds;
    ArrayList<Integer> failedIds;
    File linkedFile;
    File metaDataSyncFile;
    File errorFile;
    Document document;
    String rptFile;
    
    
     private boolean genMetaSyncedIdList () {
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE exists (" +
                        "SELECT 'X' from cdis_activity_log b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id " +
                        "AND b.cdis_status_cd = 'MS' " +
                        "AND b.activity_dt > (SYSDATE - " + this.rptDays + ")) " +
                        "ORDER BY a.file_name";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   metaSyncedIds.add(rs.getInt(1));
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain MetaData synced Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
        
    }
     
    private boolean genInProgressIdList () {
        
        //include everything in progress, regardless of date
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE exists (" +
                     "SELECT 'X' from cdis_activity_log b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id )" +
                     "AND NOT exists (" +
                        "SELECT 'X' " +
                        "FROM cdis_activity_log c " +
                        "WHERE a.cdis_map_id = c.cdis_map_id " +
                        "AND c.cdis_status_cd = 'LC')" + 
                     "AND NOT EXISTS (" + 
                        "SELECT 'X' " + 
                        "FROM CDIS_ERROR e " +
                        "WHERE a.cdis_map_id = e.cdis_map_id ) " +
                     "ORDER BY a.file_name";
  
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   inProgressIds.add(rs.getInt(1));
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain In Progress Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
    }
     
    
    private boolean genCompletedIdList () {
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE exists (" +
                        "SELECT 'X' from cdis_activity_log b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id " +
                        "AND b.cdis_status_cd = 'LC' " +
                        "AND b.activity_dt > (SYSDATE - " + this.rptDays + ")) " +
                        "ORDER BY a.file_name";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                   completedIds.add(rs.getInt(1));
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain Linked Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
        
    }
    
    private boolean genErrorIdList () {
        
        //Get the list of MAP_IDs where the mapid is the highest mapID for that file,
        // and it has an error condition, has not been deleted and is in ready state
        String sql = "SELECT cm.cdis_map_id " +
                     "FROM cdis_map cm, " +
                     "     cdis_error ce " +
                     "WHERE cm.cdis_map_id = ce.cdis_map_id " +
                     "AND cm.deleted_ind = 'N' " +
                     "AND cm.error_ind = 'Y' " +
                     "AND cm.batch_number in ( " +
                     "   SELECT max (batch_number) " +
                     "   FROM cdis_map cm2 " +
                     "    WHERE cm.File_name = cm2.File_Name) " +
                     "AND EXISTS ( " +
                     "   SELECT 'X' " +
                     "   FROM cdis_for_ingest fi " +
                     "   WHERE fi.CIS_UNIQUE_MEDIA_ID = cm.CIS_UNIQUE_MEDIA_ID " +
                     "   AND   fi.SI_HOLDING_UNIT = cm.SI_HOLDING_UNIT " +
                     "   AND   ingest_status_cd = 'RI') " +
                     "ORDER BY cm.file_name";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                while (rs.next()) {
                    failedIds.add(rs.getInt(1));            
                }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain Errored Rows, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
    }
    
    private boolean create (String siUnit) {
        
        String timeStamp;
        String timeStampWords;
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
        
        DateFormat dfWords = new SimpleDateFormat();
        timeStampWords = dfWords.format(new Date());
        
       
        this.rptFile =  "rpt\\CDISRPT-" + siUnit + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            document.add(new Paragraph(timeStampWords + "\n" + 
                                        siUnit + " CDIS Activity Report- Past " + this.rptHours + " Hours", title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
        
        return true;
    }
    
    private void statisticsWrite () {
        
        try {
            RtfFont stats=new RtfFont("Times New Roman",12);
            
            document.add(new Paragraph("\nNumber of Succesful DAMS/CIS Linkages - Completed : " + this.completedIds.size(), stats));
            document.add(new Paragraph("Number of DAMS/CIS Linkages - In Progress : " + this.inProgressIds.size(), stats));
            document.add(new Paragraph("Number of Successful MetaData Re-Synced Records: " + this.metaSyncedIds.size(), stats));
            document.add(new Paragraph("Number of Failed Records: " + this.failedIds.size(), stats));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
    }
    
    
    private void write(String recordType, ArrayList<Integer> recordList) {
        try {
            RtfFont secHeaderFont=new RtfFont("Times New Roman",12,Font.BOLD);
            
            String sectionHeader = null;
            
            switch (recordType) {
            
                case "inProgress"    :
                    sectionHeader = "\n\nIntegration Currently in Progress For: ";
                    break;
                case "completed" :
                    sectionHeader = "\n\nIntegration Successfully Completed For: ";
                    break;    
                case "metaDataSynced" :
                    sectionHeader = "\n\nMetaData Successfully Synchronized with DAMS For: ";
                    break;
                case "failed" :     
                    sectionHeader = "\n\nThe Following Media Experienced Integration Failures: ";
                    break;     
            }
            
            document.add(new Paragraph(sectionHeader,secHeaderFont));
            document.add(new Phrase("-------------------------------------------------------------------------",secHeaderFont));

        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        } 
        
        for (Iterator<Integer> iter = recordList.iterator(); iter.hasNext();) {
            
            try {
                RtfFont listElementFont=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                       
                boolean returnVal = cdisMap.populateMapInfo(damsConn);
                
                if (! returnVal ) {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                    return;
                }
                
                SiAssetMetaData siAsst = new SiAssetMetaData();
                siAsst.setUoiid(cdisMap.getDamsUoiid());
                siAsst.populateOwningUnitUniqueName(damsConn);
                
                String listing = null;
                    
                switch (recordType) {
                    case "inProgress"    :
                        listing = "FileName: " + cdisMap.getFileName() + ",  CIS Media ID: " + cdisMap.getCisUniqueMediaId() + " - Currently In Progress"; 
                        break;
                    case "completed" :
                        listing = "FileName: " + cdisMap.getFileName() + " Linked to CIS Media ID: " + cdisMap.getCisUniqueMediaId() + " DAMS UAN: " + siAsst.getOwningUnitUniqueName();
                        break;
                    case "metaDataSynced":
                        listing = "FileName: " + cdisMap.getFileName() + "MetaData Synced in DAMS: " + cdisMap.getCisUniqueMediaId() + " DAMS UAN: " + siAsst.getOwningUnitUniqueName(); 
                        break;
                    case "failed":
                        CDISErrorCodeR cdisErrorCode = new CDISErrorCodeR();            
                        returnVal = cdisErrorCode.populateDescription(damsConn, cdisMap.getCdisMapId() );
                
                        listing = "FileName: " + cdisMap.getFileName() + ",  CIS Media ID: " + cdisMap.getCisUniqueMediaId() + ",   Error: " + cdisErrorCode.getDescription() ; 
                        break;    
                    }
                             
                    document.add(new Phrase("\n" + listing,listElementFont));
            
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }
        
    }
    
    
    public void send (String siUnit, String emailTo) {
        try {
            Properties mailProps = new Properties();
            String server = "smtp.si.edu";
            mailProps.put("mail.smtp.host", server);      
            Session session = Session.getDefaultInstance( mailProps, null );

            MimeMessage message = new MimeMessage( session );
            message.setFrom(new InternetAddress("no-reply@dams.si.edu"));
            String[] toEmailAddrArray = emailTo.split(",");
            for (int i = 0; i < toEmailAddrArray.length; i++) {
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
            }
	
            String emailContent = null;
            
            message.setSubject(siUnit + ": CDIS 2.1 Activity Report" );         
            emailContent = "<br>Please see the attached CDIS Activity Report<br><br><br><br>" +
                    "If you have any questions regarding information contained in this report, please contact: <br>" + 
                    "Robert Feldman (FeldmanR@si.edu) or Isabel Meyer (MeyerI@si.edu <br><br><br><br>" + 
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
    
    
    public void generate (CDIS cdis) {
        
        this.damsConn = cdis.damsConn;
        
        try {
            this.rptHours = cdis.properties.getProperty("rptHours");
            this.rptDays = Double.parseDouble(rptHours) / 24;
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to calculate timeframe of report, defaulting to last 24 hours");
            this.rptDays = 1.0;
            this.rptHours = "24";
        }        
        
        create(cdis.properties.getProperty("siHoldingUnit"));
         
        //Get list of completed records (UOI_IDs) from the past increment
        // In progress should be collected first in case there are some currently in progress while the report is running
        // ...we dont want to miss the ones that may be in progress now but completed by the time we get to the next step.
        this.inProgressIds = new ArrayList<>();
        genInProgressIdList ();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.completedIds = new ArrayList<>();
        genCompletedIdList ();

        //Get the metadata synced records in the last increment
        this.metaSyncedIds = new ArrayList<>();
        genMetaSyncedIdList ();
          
        //Get the failed records from the past increment
        this.failedIds = new ArrayList<>();
        genErrorIdList ();
        
        statisticsWrite();
        if (completedIds.size() > 0 ) {
            write("completed", this.completedIds);
        }
       
        if (inProgressIds.size() > 0 ) {
            write("inProgress", this.inProgressIds);
        }
        
        
        //Loop through the metadata sync list and generate report
        if (metaSyncedIds.size() > 0) {
            write("metaDataSynced", this.metaSyncedIds);
        }
        
        if (failedIds.size() > 0) {
            write("failed", this.failedIds);
        }
        
        //close the Document
        document.close();
        
        if (cdis.properties.getProperty("emailReportTo") != null) { 
            //send email to list
            logger.log(Level.FINEST, "Need To send Email Report");
            
            send(cdis.properties.getProperty("siHoldingUnit"), cdis.properties.getProperty("emailReportTo") );
        }
        
    }
}
