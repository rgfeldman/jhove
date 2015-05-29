/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS;

import edu.si.CDIS.CIS.Database.CDISTable;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.File;
import java.util.Properties;
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
import org.apache.commons.io.FileUtils;


public class StatisticsReport {
    
    File successFile;
    File failFile;
    File headerFile;
    File reportFile;
    
    PrintWriter successFileWrt;
    PrintWriter failFileWrt;
    PrintWriter headerFileWrt;
    
    String timestamp;
    
    // This is the default constructor...initialize the files
    StatisticsReport () {
        
        //get the date timestamp for use in the report file
        DateFormat df = new SimpleDateFormat("yyyyMMdd-kkmmss");
        this.timestamp = df.format(new Date());
        
        this.successFile = new File("tmp\\success_" + this.timestamp + ".rtf");
        this.failFile = new File("tmp\\fail_" + this.timestamp + ".rtf");
        this.headerFile = new File("tmp\\header_" + this.timestamp + ".rtf");
        
        try {
            this.successFileWrt = new PrintWriter(new BufferedWriter(new FileWriter(successFile, true)));
            this.failFileWrt = new PrintWriter(new BufferedWriter(new FileWriter(failFile, true)));
            this.headerFileWrt = new PrintWriter(new BufferedWriter(new FileWriter(headerFile, true)));
            
         } catch (Exception e) {
            e.printStackTrace();
            this.successFileWrt.close();
            this.failFileWrt.close();
        }
    }
    
    
    
    // Write out stats to the report file for this run
    //public void writeUpdateStats(CDISTable cdisTbl, String syncType, boolean successFlag) {
    public void writeUpdateStats(String uoiid, String renditionNumber, String syncType, boolean successFlag) {
        if (successFlag) {
            this.successFileWrt.append(uoiid + " " + syncType + " " + renditionNumber + "\n");
        } else {
            this.failFileWrt.append(uoiid + " " + syncType + " " + renditionNumber + "\n");
        }
    }
        
    public void populateHeader (String siUnit, String operationType) {
        
        if (operationType.equals("ingestToCIS") ) {
            this.headerFileWrt.append ("CDIS 2.0: DAMS to TMS: Media Creation Report and Statistics\n");
        }
        else if (operationType.equals("ingestToDams") ) {
            this.headerFileWrt.append ("CDIS 2.0: Collections to DAMS: Image Creation Report and Statistics\n");
        }
        else if (operationType.equals("linkToCIS") ) {
            this.headerFileWrt.append ("CDIS 2.0: DAMS/Collections Link Report and Statistics\n");
        }
        else if (operationType.equals("sync") ) {
            this.headerFileWrt.append ("CDIS 2.0: Synchronization Report and Statistics\n");
        }
        else if (operationType.equals("thumbnailSync") ) { 
            this.headerFileWrt.append ("CDIS 2.0: Thumbnail Synchronization Report and Statistics\n");
        }
        this.headerFileWrt.append ("siUnit: " + siUnit + "\n");
        this.headerFileWrt.append ("Batch Number: " + this.timestamp + "\n\n\n");
    }
    
    public void populateStats (int neverSyncedSize, int sourceUpdatedSize, int successCount, int failCount, String operationType) {
                
        if (operationType.equals("meta")) {
            // Get count of number of Renditions to Sync, and send to Report File
            this.headerFileWrt.append("DAMS images that have not been synced before: " + neverSyncedSize + "\n");
            this.headerFileWrt.append("DAMS images that need updated metadata: " + sourceUpdatedSize + "\n");
        
            int TotalRend = neverSyncedSize + sourceUpdatedSize;
        
            this.headerFileWrt.append("Total Number of Images to Metadata sync: " + TotalRend + "\n");
            this.headerFileWrt.append("Total Number of Successful Metadata Updates: " + successCount + "\n");
            this.headerFileWrt.append("Total Number of Failed Metadata Updates: " + failCount + "\n\n\n");
        }
        else if (operationType.equals("ids")) {
            this.headerFileWrt.append("Renditions to IDS path sync in CIS: " + neverSyncedSize + "\n");
            this.headerFileWrt.append("Total Number of Successful updates to point to IDS: " + successCount + "\n\n\n");
        }
        else if (operationType.startsWith("ingest")) {
            this.headerFileWrt.append("Total Number of Successful Ingest Records: " + successCount + "\n");
            this.headerFileWrt.append("Total Number of Failed Ingest Records: " + failCount + "\n\n\n");
        }
        else if (operationType.equals("linkToCIS")) {
             this.headerFileWrt.append("Unlinked DAMS Renditions: " + neverSyncedSize + "\n\n\n");
             this.headerFileWrt.append("Total Number of Successful Links: " + successCount + "\n");
             this.headerFileWrt.append("Total Number of Failed Links: " + failCount + "\n");
        }
        
    }
    
    // Instead of putting the report file from a buffer, and sending them out, I have opted for this approach.
    // It is more cumbersome, and more flexibile...but in the end more reliable.
    // Advantage of having individual files is they can be compiled and mailed at the end of the work day, or can be sent at batch processing time.
    // Also if the process is not read from a buffer, but actual files,
    // if the application comes down, the files will still be there (hopefully)
    public void compile (String operationType) {
        
        // make sure all the fileWriters are closed first
        failFileWrt.close();
        headerFileWrt.close();
        successFileWrt.close();
        
        // File to write
        reportFile = new File("rpt\\Rpt_" + operationType + this.timestamp + ".rtf");

        try {
            
            // Read the file like string
            if(this.headerFile.exists()){
                String headerStr = FileUtils.readFileToString(this.headerFile);
                FileUtils.writeStringToFile(reportFile, headerStr, true);
            }
            
            String sucessStr = null;
            if(this.successFile.exists()){
                
                sucessStr = FileUtils.readFileToString(this.successFile);
                FileUtils.writeStringToFile(reportFile, "================================================================\n", true);
                if (operationType.equals("ingestToDAMS")) {
                    FileUtils.writeStringToFile(reportFile, "Files Sent to DAMS:\n", true);
                }
                else {
                     FileUtils.writeStringToFile(reportFile, "UAN / Rendition Number Pairs:\n", true);
                }
            
                if (this.successFile.length() > 0 ) {
                    String successStr = FileUtils.readFileToString(this.successFile); 
                    FileUtils.writeStringToFile(reportFile, successStr, true);
                }
                else {
                    FileUtils.writeStringToFile(reportFile, "No Records synced/ingested in this batch\n", true);
                }
            }
            
            String failStr = null;
            if(this.failFile.exists()) {
                failStr = FileUtils.readFileToString(this.failFile);
                FileUtils.writeStringToFile(reportFile, "================================================================\n", true);
                if (operationType.equals("ingestToDAMS")) {
                    FileUtils.writeStringToFile(reportFile, "Failed FileNames\n", true);
                }
                else {
                    FileUtils.writeStringToFile(reportFile, "Failed UAN / Rendition Number Pairs:\n", true);
                }
                
                if (this.failFile.length() > 0 ) { 
                    failStr = FileUtils.readFileToString(this.failFile);
                    FileUtils.writeStringToFile(reportFile, failStr, true);
                }
                else {
                    FileUtils.writeStringToFile(reportFile, "No Failures recorded\n", true);
                }
            }       
            
        } catch (Exception e) {
            e.printStackTrace();
        } 
        
    }
    
    public void send (String siUnit, String emailTo, String operationType) {
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
            
            if (operationType.equals("sync")) {
                message.setSubject("CDIS 2.0: " + siUnit + " Metadata Sync Report for batch number " + timestamp);         
                emailContent = "<br>"+"Please see the attached CDIS Metadata Sync report for "+ timestamp + "<br>";
            }
            else if (operationType.equals("linkToCIS")) {
                message.setSubject("CDIS 2.0: " + siUnit + " DAMS/CIS Link Report for batch number " + timestamp);         
                emailContent = "<br>"+"Please see the attached CDIS Link report for "+ timestamp + "<br>";
            }
            else if (operationType.equals("ingestToCIS")) {
                message.setSubject("CDIS 2.0: " + siUnit + " TMS Media Creation Report for batch number " + timestamp);         
                emailContent = "<br>"+"Please see the attached CDIS TMS Ingest report for "+ timestamp + "<br>";                  
            }
            else if (operationType.equals("ingestToDAMS")) {
                message.setSubject("CDIS 2.0: " + siUnit + " Ingest to DAMS Report for batch number " + timestamp);         
                emailContent = "<br>"+"Please see the attached CDIS DAMS Ingest report for "+ timestamp + "<br>";              
            }
            else if (operationType.equals("thumbnailSync")) {
                message.setSubject("CDIS 2.0: " + siUnit + " TMS Thumbnail Sync for batch number " + timestamp);
            }
            else { 
                System.out.println("Error, invalid operationType for Report");
                emailContent = "<br>"+"Please see the attached CDIS DAMS Ingest report for "+ timestamp + "<br>"; 
            } 
                
		// create the Multipart and its parts to it
            Multipart parts = new MimeMultipart();

		// create and fill the Body
           
            MimeBodyPart bodyPart = new MimeBodyPart();
            bodyPart.setContent(emailContent,"text/html");
            parts.addBodyPart(bodyPart);
		
		//add the attachment
            MimeBodyPart attachmentPart = new MimeBodyPart();
	    DataSource source = new FileDataSource(reportFile);
	    attachmentPart.setDataHandler(new DataHandler(source));
	    attachmentPart.setFileName(this.reportFile.getName());
	    parts.addBodyPart(attachmentPart);

            // add the Multipart to the message
            message.setContent(parts);
            Transport.send(message);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
