/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.utilities;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuActivityLog;
import edu.si.damsTools.vfcu.database.VfcuErrorLog;
import edu.si.damsTools.vfcu.database.VfcuMd5FileActivityLog;
import edu.si.damsTools.vfcu.database.VfcuMd5FileError;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    public void captureMediaFileError (VfcuMediaFile vfcuMediaFile, String errorCode, String addlInfo, String logMessage) {
        
        logger.log(Level.FINER, logMessage);
        
        VfcuErrorLog vfcuError = new VfcuErrorLog();
        
        vfcuError.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        vfcuError.setFileName(vfcuMediaFile.getMediaFileName());
        vfcuError.setVfcuMd5FileId(vfcuMediaFile.getVfcuMd5FileId());
        vfcuError.setAddlErrorInfo(addlInfo);
        vfcuError.setVfcuErrorCd(errorCode);
        vfcuError.insertRecord();
        
        VfcuActivityLog vfcuActivityLog = new VfcuActivityLog();
        vfcuActivityLog.setVfcuMediaFileId(vfcuMediaFile.getVfcuMediaFileId());
        vfcuActivityLog.setVfcuStatusCd("ER");
        vfcuActivityLog.insertRow();
    }    
    
    public void captureMd5Error (VfcuMd5File vfcuMd5File, String errorCode, String logMessage) {
        
        logger.log(Level.FINER, logMessage);
        
        VfcuMd5FileError vfcuMd5Error = new VfcuMd5FileError();
        
        vfcuMd5Error.setVfcuMd5FileId(vfcuMd5File.getVfcuMd5FileId());
        vfcuMd5Error.setVfcuMd5ErrorCd(errorCode);
        vfcuMd5Error.insertRecord();
        
        VfcuMd5FileActivityLog vfcuMd5ActivityLog = new VfcuMd5FileActivityLog();
        vfcuMd5ActivityLog.setVfcuMd5FileId(vfcuMd5File.getVfcuMd5FileId());
        vfcuMd5ActivityLog.setVfcuMd5StatusCd("ER");
        vfcuMd5ActivityLog.insertRecord();
    }    
        
}
