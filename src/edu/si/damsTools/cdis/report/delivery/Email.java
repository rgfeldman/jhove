/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.report.rptFile.RptFile;
import edu.si.damsTools.cdis.operations.report.DisplayFormat;
import java.io.File;
import java.util.Properties;
import java.util.logging.Level;
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
import java.util.logging.Logger;

/**
 * Class: Email
 * Purpose: The contains the methods related to delivering report file via email.
 *
 * @author rfeldman
 */
public class Email implements DeliveryMethod {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    //public void composeSend (String atchmntFileName) {
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
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
            
            String emailTitle = displayFormat.returnEmailTitle(rptFile.getKeyValue());
            if (emailTitle != null ) {
                message.setSubject(emailTitle);
            }
            else {
                logger.log(Level.FINEST, "No email title, will not generate report.  Returning");    
                return false;
            }

            for (String statLine : rptFile.getStatsList()) {
                emailContent = emailContent + statLine + "<br>";
            }
            
            // create the Multipart and its parts to it
            Multipart parts = new MimeMultipart();

            // create the Body 
            MimeBodyPart bodyPart = new MimeBodyPart();

            if (! displayFormat.returnSupressAttachFlag(rptFile.getKeyValue())) { //|| (this.failedIdName.size() > 0) {
                emailContent = emailContent + "<br>Please see the attached CDIS Activity Report for details <br>";
          
                //add the attachment
                File reportFile = new File (rptFile.getFileNameLoc());
                   
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
            return false;
        }
        
        return true;
    }
}
