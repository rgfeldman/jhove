/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.DAMS.Database.SiAssetMetaData;
import edu.si.CDIS.Database.CDISError;
import edu.si.CDIS.Database.CDISMap;
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
        }  
        
        return true;
    }
    
    
    public void generate () {
        
        try {
            this.rptHours = CDIS.getProperty("rptHours");
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to calculate timeframe of report, defaulting to last 24 hours");
            this.rptHours = "24";
        }
        
        create();
        
        //Get list of completed records (UOI_IDs) from the past increment
        this.completedIdName = new LinkedHashMap<>();
        genCompletedIdList ();
     
        //Get the failed records from the past increment
        this.failedIdName = new LinkedHashMap<>();
        genFailedIdList ();
        
        statisticsWrite();
                
        //failed list is to be displayed before successful list per Ken
        writeFailed();
                
        //now write successful completion list to file
        writeCompleted();

        //close the Document
        document.close();
        
        if (CDIS.getProperty("emailReportTo") != null) { 
            //send email to list
            logger.log(Level.FINEST, "Need To send Email Report");
            
            send(CDIS.getProperty("siHoldingUnit"), CDIS.getProperty("emailReportTo") );
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
    
    private boolean genCompletedIdList () {
        
        String sqlTypeArr[] = null;
        String sql = null;
        
        for (String key : CDIS.getXmlSelectHash().keySet()) {     
            
            sqlTypeArr = CDIS.getXmlSelectHash().get(key);
            
                if (sqlTypeArr[0].equals("getSuccessRecords")) {   
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
                this.completedIdName.put(rs.getInt(1), rs.getString(2));    
            }        
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Obtain list for report, returning", e);
                return false;
	}
        
        return true;
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
            
            message.setSubject(siUnit + ": CDIS Activity Report - Past " + this.rptHours + " Hours" ); 
            
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
            
            sectionHeader = "\n\nThe Following Media Successfully Copied, Validated and Recorded: ";
                          
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
                
                CDISMap cdisMap = new CDISMap();
                SiAssetMetaData siAsst = new SiAssetMetaData();
                
                cdisMap.setCdisMapId(mapId);
                cdisMap.populateMapInfo();
                
                siAsst.setUoiid(cdisMap.getDamsUoiid());
                siAsst.populateOwningUnitUniqueName();
                      
                RtfFont listElementFont=new RtfFont("Courier",8);
                
                String listing = "File: " + cdisMap.getFileName() + "  Linked To UAN: " + siAsst.getOwningUnitUniqueName() + " eMu id: " + cdisMap.getCisUniqueMediaId() ;
                                         
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

            sectionHeader = "\n\nThe Following Media Experienced Integration Failures: ";
            
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
