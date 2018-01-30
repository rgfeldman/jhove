/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.delivery;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.report.DisplayFormat;
import edu.si.damsTools.cdis.report.rptFile.RptFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.utilities.ExecSystemCmd;



/**
 *
 * @author rfeldman
 */
public class Sftp implements DeliveryMethod{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public boolean deliver (DisplayFormat displayFormat, RptFile rptFile) {
        
        try{            
            String pathName = rptFile.getFileNameLoc().substring(0,rptFile.getFileNameLoc().lastIndexOf("/") );
            String fileName = rptFile.getFileNameLoc().substring(rptFile.getFileNameLoc().lastIndexOf("/")+1);

            logger.log(Level.FINEST, "Putting Report file " + rptFile.getFileNameLoc().toString() ); 
            
            //run the script that has the commands in it
            String returnVal = ExecSystemCmd.execCmd ("sftpRpt.ksh " + pathName + " " + fileName );
            
            boolean ftpValidated = validateFtpMsg (returnVal, fileName);
            if (!ftpValidated) {
                logger.log(Level.FINEST,"Error withing ftp process");
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
    
    private boolean validateFtpMsg(String ftpMsg, String fileName) {
        String ftpMsgs[] = ftpMsg.split("\n");
        
        int val = 0;
        
        for (int i = 0; i < ftpMsgs.length; i++) {
            
            if (ftpMsgs[i].startsWith("Uploading")) {
                val ++;
            }
            if (ftpMsgs[i].startsWith("sftp> ls " + fileName)) {
               val ++;
            }
            if (ftpMsgs[i].startsWith(fileName)) {
                val ++;
            }
            if (ftpMsgs[i].startsWith("sftp> bye")) {
                val ++;
            }

        }
        return val == 4;
    }

}
