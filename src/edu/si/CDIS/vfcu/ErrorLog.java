/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.vfcu;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.vfcu.Database.VFCUActivityLog;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.vfcu.Database.VFCUError;
import edu.si.CDIS.vfcu.Database.VFCUMediaFile;

public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    public void capture (VFCUMediaFile vfcuMediaFile, String errorCode, String logMessage, Connection damsConn) {
        
        logger.log(Level.FINER, logMessage);
        
        VFCUError vfcuError = new VFCUError();
        vfcuError.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        vfcuError.setVfcuErrorCd(errorCode);
        vfcuError.insertRecord(damsConn);
        
        VFCUActivityLog vfcuActivityLog = new VFCUActivityLog();
        vfcuActivityLog.setVfcuStatusCd("ER");
        vfcuActivityLog.insertRow(damsConn);
        
    }
    
}
