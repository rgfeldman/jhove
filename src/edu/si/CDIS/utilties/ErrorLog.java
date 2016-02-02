/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.utilties;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISError;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISMap;


public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    public void capture (CDISMap cdisMap, String errorCode, String logMessage) {
        
        logger.log(Level.FINER, logMessage);
        
        CDISError cdisError = new CDISError();
        cdisError.setCdisMapId(cdisMap.getCdisMapId());
        cdisError.setFileName(cdisMap.getFileName());
        cdisError.setCdisErrorCd(errorCode);
        cdisError.insertError();
        
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("ERR");   
        cdisActivity.insertActivity(); 
    }
    
}
