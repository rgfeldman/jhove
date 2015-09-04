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

/**
 *
 * @author rfeldman
 */
public class ErrorLog {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    public void capture (Integer cdisMapId, String errorCode, String logMessage, Connection damsConn) {
        
        logger.log(Level.FINER, logMessage);
        
        CDISError cdisError = new CDISError();
        cdisError.insertError(damsConn, "STDI", cdisMapId, errorCode);
        
        CDISActivityLog cdisActivity = new CDISActivityLog();
        cdisActivity.insertActivity(damsConn, cdisMapId, "ER");
    }
    
}
