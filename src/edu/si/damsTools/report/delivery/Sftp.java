/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.ExecSystemCmd;
import edu.si.damsTools.report.DisplayFormat;
import edu.si.damsTools.report.rptFile.RptFile;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Class : Sftp
 * Purpose: This class contains methods that are used to deliver a report file via sftp. 
 *
 * @author rfeldman
 */
public class Sftp implements DeliveryMethod{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
        
        try{
            // derive the pathname and the filename from the filename and path concatenation 
            String pathName = rptFile.getFileNameLoc().substring(0,rptFile.getFileNameLoc().lastIndexOf('/') );
            String fileName = rptFile.getFileNameLoc().substring(rptFile.getFileNameLoc().lastIndexOf('/')+1);

            logger.log(Level.FINEST, "Putting Report file " + rptFile.getFileNameLoc() ); 
            logger.log(Level.FINEST, "sftpRpt.sh "  + pathName + " " + fileName ); 
            
            //run the script that has the commands in it
            String returnVal = ExecSystemCmd.execCmd ("sftpRpt.sh " + pathName + " " + fileName );
            
            // make sure the sftp was successful
            boolean ftpValidated = validateFtpMsg (returnVal, fileName);
            if (!ftpValidated) {
                logger.log(Level.FINEST,"Error within ftp process");
                return false;
            }
            
            logger.log(Level.FINEST,"Successfully validated ftp send");
            
        }
        catch (Exception e) {
             System.out.println("Error, unable to ftp files");
             return false;
        }
        
        return true;
    }
    
    /* 
    /* Method : validateFtpMsg
    /* Purpose: pareses the ftp output that was returned from the ahell script to ensure sfp was completed successfully 
    */
    
    private boolean validateFtpMsg(String ftpMsg, String fileName) {
        //split the multiline output from the sftp script into individual lines
        
        String ftpMsgs[] = ftpMsg.split("\n");
        
        int val = 0;
        
        //verify information by looking for key words in return message from Sftp
        for (String msg : ftpMsgs) {
            if (msg.startsWith("Uploading")) {
                val ++;
            }
            if (msg.startsWith("sftp> ls " + fileName)) {
                val ++;
            }
            if (msg.startsWith(fileName)) {
                val ++;
            }
            if (msg.startsWith("sftp> bye")) {
                val ++;
            }
        }
        //make sure we have met all 4 conditions, otherwise return a false value
        return val == 4;
    }

}
