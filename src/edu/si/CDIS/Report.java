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
                     "WHERE metadata_sync_dt > (SYSDATE - " + this.rptDays + ")" +
                     "AND integration_complete_dt < (SYSDATE - " + this.rptDays + ")" +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' FROM cdis_error b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id)";
        
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
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE integration_complete_dt is NULL " +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' FROM cdis_error b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id)";
        
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
                     "WHERE integration_complete_dt > (SYSDATE - " + this.rptDays + ")" +
                     "AND NOT EXISTS (" +
                        "SELECT 'X' FROM cdis_error b " +
                        "WHERE a.cdis_map_id = b.cdis_map_id)";
        
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
        
        String sql = "SELECT cdis_map_id FROM cdis_map a " + 
                     "WHERE EXISTS ( " + 
                            "SELECT 'X' from cdis_error b " +
                            "WHERE a.cdis_map_id = b.cdis_map_id " +
                            "AND b.error_dt > (SYSDATE - " + this.rptDays + "))";
        
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
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        timeStamp = df.format(new Date());
       
        this.rptFile =  "rpt\\CDISRPT-" + siUnit + "-" + timeStamp + ".rtf";
        
        this.document = new Document();
         
        try {
            
            RtfWriter2.getInstance(document,new FileOutputStream(rptFile));
       
            document.open();
            
            RtfFont title=new RtfFont("Times New Roman",14,Font.BOLD);
            document.add(new Paragraph(siUnit + " CDIS Activity Report - Past " + this.rptHours + " Hours", title));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
        
        return true;
    }
    
    private void statisticsWrite () {
        
        try {
            RtfFont stats=new RtfFont("Times New Roman",12);
            
            document.add(new Paragraph("\nNumber of Succesful DAMS/CIS Linkages - Completed : " + this.completedIds.size(), stats));
            document.add(new Paragraph("Number of Succesful DAMS/CIS Linkages - In Progress : " + this.inProgressIds.size(), stats));
            document.add(new Paragraph("Number of Successful MetaData Re-Synced Records: " + this.metaSyncedIds.size(), stats));
            document.add(new Paragraph("Number of Failed Records: " + this.failedIds.size(), stats));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
    }
    
    private void completedWrite() {
        
        try {
            RtfFont secHeader=new RtfFont("Times New Roman",12,Font.BOLD);
            
            document.add(new Paragraph("\n\n----------------------------------------------------",secHeader));
            document.add(new Paragraph("\nIntegration Successfully Completed for"));
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
            
        for (Iterator<Integer> iter = completedIds.iterator(); iter.hasNext();) {
            
            try {
                RtfFont stats=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                       
                boolean returnVal = cdisMap.populateMapInfo(damsConn);
                
                if ( returnVal ) { 
                
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    siAsst.setUoiid(cdisMap.getUoiid());
                    siAsst.populateSourceSystemId(damsConn);
                
                    String listing = "FileName: " + cdisMap.getFileName() + "Linked to CisID: " + cdisMap.getCisId() + " Source System ID: " + siAsst.getSourceSystemId(); 
                                
                    document.add(new Paragraph("\n" + listing,stats));
            
                }
                else {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                }
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }
        
    }
    
    private void  inProgressWrite() {
        try {
            RtfFont secHeader=new RtfFont("Arial",10);
            document.add(new Paragraph("\n\n----------------------------------------------------",secHeader));
            document.add(new Paragraph("\nIntegration Currently in Progress For"));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
        
        for (Iterator<Integer> iter = inProgressIds.iterator(); iter.hasNext();) {
            
            try {
                RtfFont stats=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                       
                boolean returnVal = cdisMap.populateMapInfo(damsConn);
                
                if ( returnVal ) { 
                
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    siAsst.setUoiid(cdisMap.getUoiid());
                    siAsst.populateSourceSystemId(damsConn);
                
                    String listing = "FileName: " + cdisMap.getFileName() + "MetaData Synced in DAMS: " + cdisMap.getCisId() + " Source System ID: " + siAsst.getSourceSystemId(); 
                                
                    document.add(new Paragraph("\n" + listing,stats));
            
                }
                else {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                }
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }
    }
    
    private void syncedWrite() {
        
        try {
            RtfFont secHeader=new RtfFont("Arial",10);
            document.add(new Paragraph("\n\n----------------------------------------------------",secHeader));
            document.add(new Paragraph("\nMetaData Successfully Synchronized with DAMS for"));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
        
        for (Iterator<Integer> iter = metaSyncedIds.iterator(); iter.hasNext();) {
            
            try {
                RtfFont stats=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                       
                boolean returnVal = cdisMap.populateMapInfo(damsConn);
                
                if ( returnVal ) { 
                
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    siAsst.setUoiid(cdisMap.getUoiid());
                    siAsst.populateSourceSystemId(damsConn);
                
                    String listing = "FileName: " + cdisMap.getFileName() + "MetaData Synced in DAMS: " + cdisMap.getCisId() + " Source System ID: " + siAsst.getSourceSystemId(); 
                                
                    document.add(new Paragraph("\n" + listing,stats));
            
                }
                else {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                }
            } catch(Exception e) {
                logger.log(Level.FINEST, "ERROR",e);
            }
        }
        
    }

    private void failedWrite() {
        
         try {
            RtfFont secHeader=new RtfFont("Arial",10);
            document.add(new Paragraph("\n\n\n\n\n----------------------------------------------------",secHeader));
            document.add(new Paragraph("\nMedia with Integration Failures"));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "ERROR",e);
        }  
        
        for (Iterator<Integer> iter = metaSyncedIds.iterator(); iter.hasNext();) {
            
            try {
                RtfFont stats=new RtfFont("Arial",10);
            

                CDISMap cdisMap = new CDISMap();
                cdisMap.setCdisMapId(iter.next());
                boolean returnVal = cdisMap.populateMapInfo(damsConn);
                
                CDISErrorCodeR cdisErrorCode = new CDISErrorCodeR();            
                returnVal = cdisErrorCode.populateDescription(damsConn, cdisMap.getCdisMapId() );
                
                if ( returnVal ) { 
                
                    SiAssetMetaData siAsst = new SiAssetMetaData();
                    siAsst.setUoiid(cdisMap.getUoiid());
                
                    String listing = "FileName: " + cdisMap.getFileName() + "CIS ID: " + cdisMap.getCisId() + " Error: " + cdisErrorCode.getDescription() ; 
                                
                    document.add(new Paragraph("\n" + listing,stats));
            
                }
                else {
                    logger.log(Level.FINEST, "ERROR in obtaining map data for Report");
                }
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
        
        create(cdis.properties.getProperty("siUnit"));
         
        //Get list of completed records (UOI_IDs) from the past increment
        // In progress should be collected first in case there are some currently in progress while the report is running
        // ...we dont want to miss the ones that may be in progress now but completed by the time we get to the next step.
        this.inProgressIds = new ArrayList<>();
        genInProgressIdList ();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.completedIds = new ArrayList<>();
        genCompletedIdList ();

        //Get the metadata synced records from the past increment
        this.metaSyncedIds = new ArrayList<>();
        genMetaSyncedIdList ();
        
        //Get the failed records from the past increment
        this.failedIds = new ArrayList<>();
        genErrorIdList ();
        
        statisticsWrite();
        if (completedIds.size() > 0 ) {
            completedWrite();
        }
       
        if (inProgressIds.size() > 0 ) {
            inProgressWrite();
        }
        
        //Loop through the metadata sync list and generate report
        if (metaSyncedIds.size() > 0) {
            syncedWrite ();
        }
        
        if (failedIds.size() > 0) {
            failedWrite();
        }
        
        //close the Document
        document.close();
        
        if (cdis.properties.getProperty("emailReportTo") != null) { 
            //send email to list
            logger.log(Level.FINEST, "Need To send Email Report");
            
            send(cdis.properties.getProperty("siUnit"), cdis.properties.getProperty("emailReportTo") );
        }
        
    }
}
