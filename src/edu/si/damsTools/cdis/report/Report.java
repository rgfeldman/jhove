/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report;

import edu.si.damsTools.cdis.report.DisplayFormat;

import edu.si.damsTools.cdis.report.attachment.Attachment;
import edu.si.damsTools.DamsTools;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
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


/**
 *
 * @author rfeldman
 */
public class Report extends Generator {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected static String keyValue;
    
    public Report() {
         super();
    }
    
    public void setKeyValue (String keyValue) {
        this.keyValue = keyValue;
    }
    
    public boolean generate () {

        Attachment attach = new Attachment();
        attach.setKeyValue(keyValue);
        
        //Initiatlize file for attachment and create the header           
        attach.createFileAndHeader(displayFormat);
 
         //Obtain the data for all the sections
        attach.obtainSectionData(displayFormat);
        
        attach.closeDocument();
        
        logger.log(Level.FINEST, "Attachment fileName: " + attach.getFileNameLoc());  
        
         //Craete the actual email and send the email
         composeSend(attach);

        
        return true;
    }
    
    
    //public void composeSend (String atchmntFileName) {
    public void composeSend (Attachment attach) {
        try {
            Properties mailProps = new Properties();
            String server = "smtp.si.edu";
            mailProps.put("mail.smtp.host", server);      
            Session session = Session.getDefaultInstance( mailProps, null );
            MimeMessage message = new MimeMessage( session );
            message.setFrom(new InternetAddress("no-reply@dams.si.edu"));
            
            String emailContent = "";

            String[] toEmailAddrArray = displayFormat.returnEmailToList().split(",");
            for (int i = 0; i < toEmailAddrArray.length; i++) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i].trim()));
            }
            
            String emailTitle = displayFormat.returnEmailTitle(this.keyValue);
            if (emailTitle != null ) {
                message.setSubject(emailTitle);
            }
            else {
                logger.log(Level.FINEST, "No email title, will not generate report.  Returning");    
                return;
            }

            for (String statLine : attach.getStatsList()) {
                emailContent = emailContent + statLine + "<br>";
            }
            
            // create the Multipart and its parts to it
            Multipart parts = new MimeMultipart();

            // create the Body 
            MimeBodyPart bodyPart = new MimeBodyPart();

            if (! displayFormat.returnSupressAttachFlag(keyValue)) { //|| (this.failedIdName.size() > 0) {
                emailContent = emailContent + "<br>Please see the attached CDIS Activity Report for details <br>";
          
                //add the attachment
                File reportFile = new File (attach.getFileNameLoc());
                   
                MimeBodyPart attachmentPart = new MimeBodyPart();
                DataSource source = new FileDataSource(reportFile);
                attachmentPart.setDataHandler(new DataHandler(source));
                attachmentPart.setFileName(reportFile.getName());
                parts.addBodyPart(attachmentPart);
            }
            
            emailContent = emailContent + "<br><br><br>If you have any questions regarding information contained in this report, please contact: <br>" + 
                "Robert Feldman (FeldmanR@si.edu) or Isabel Meyer (MeyerI@si.edu) <br><br><br><br>" + 
                "Please let us know if someone else in your organization requires this information, or if you would like for us to discontinue delivery of this report to you.<br>" +
                "Thanks! The DAMS team";
            
            bodyPart.setContent(emailContent,"text/html");
            parts.addBodyPart(bodyPart);
		
            // add the Multipart to the message
            message.setContent(parts);
            Transport.send(message);
            
        } catch (Exception e) {
            logger.log(Level.FINEST, "Exception...error in composeEmail ", e);
        }
    }

}
