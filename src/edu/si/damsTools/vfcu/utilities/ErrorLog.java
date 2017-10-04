/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.utilities;

import edu.si.damsTools.vfcu.database.VFCUActivityLog;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.vfcu.database.VFCUErrorLog;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.DamsTools;

public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    public void capture (VfcuMediaFile vfcuMediaFile, String errorCode, String logMessage) {
        
        logger.log(Level.FINER, logMessage);
        
        VFCUErrorLog vfcuError = new VFCUErrorLog();
        
        vfcuError.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        vfcuError.setFileName(vfcuMediaFile.getMediaFileName());
        vfcuError.setVfcuMd5FileId(vfcuMediaFile.getVfcuMd5FileId());
        vfcuError.setVfcuErrorCd(errorCode);
        vfcuError.insertRecord();
        
        VFCUActivityLog vfcuActivityLog = new VFCUActivityLog();
        vfcuActivityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        vfcuActivityLog.setVfcuStatusCd("ER");
        vfcuActivityLog.insertRow();
    }    
}
