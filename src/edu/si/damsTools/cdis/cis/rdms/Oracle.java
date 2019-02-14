/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.rdms;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class Oracle implements Rdms {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());    

    
    public String returnDbDateTime(Integer numDays) {
        
        String timestamp = null;
        String numDaysStr;       
        if (numDays == null) {
            numDaysStr = "";
        }
        else {
            numDaysStr = numDays.toString();       
        }
        
        String sql = "SELECT TO_CHAR(SYSDATE " + numDaysStr + ",'YYYY-MM-DD HH24:MI:SS') " +
                    "FROM dual";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        //workaround in case DAMS is the cis as well
        Connection dbConn;
        if (DamsTools.getCisConn() != null) {
            dbConn = DamsTools.getCisConn();
        }
        else {
            dbConn = DamsTools.getDamsConn();
        }
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                timestamp =  (rs.getString(1));
            }  
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain timestamp from DB", e );
        }
        return timestamp;
    }
}
