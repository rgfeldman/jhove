/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdisutilities;

import edu.si.damsTools.cdis.database.CdisErrorLog;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;


public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    public void capture (CdisMap cdisMap, String errorCode, String logMessage) {
        
        logger.log(Level.FINER, logMessage);
        
        //populate the filename and mapinfo if we need it
        cdisMap.populateMapInfo();
        
        CdisErrorLog cdisError = new CdisErrorLog();
        cdisError.setCdisMapId(cdisMap.getCdisMapId());
        cdisError.setFileName(cdisMap.getFileName());
        cdisError.setCdisErrorCd(errorCode);
        cdisError.insertError();
        
        CdisActivityLog cdisActivity = new CdisActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("ERR");   
        cdisActivity.insertActivity(); 
    }
    
}
