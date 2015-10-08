/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.utilties;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.CDISError;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import edu.si.CDIS.DAMS.Database.CDISActivityLog;
import edu.si.CDIS.DAMS.Database.CDISMap;


public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    public void capture (CDISMap cdisMap, String errorCode, String logMessage, Connection damsConn) {
        
        logger.log(Level.FINER, logMessage);
        
        CDISError cdisError = new CDISError();
        cdisError.insertError(damsConn, cdisMap.getCdisMapId(), errorCode);
        
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.setCdisMapId(cdisMap.getCdisMapId());
        cdisActivity.setCdisStatusCd("ER");   
        cdisActivity.insertActivity(damsConn);
        
        cdisMap.setErrorInd('Y');
        cdisMap.updateErrorInd(damsConn);
        
    }
    
}
