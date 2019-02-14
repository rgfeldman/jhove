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

/**
 *
 * @author rfeldman
 */
public class Sybase implements Rdms {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());    
    
        public String returnDbDateTime(Integer numDays) {
                
        String numDaysStr = numDays.toString();
        
        if (numDaysStr == null) {
            numDaysStr = "0";
        }
        String timestamp = null;
        
        String sql = "SELECT CONVERT(varchar, dateadd(dd, " + numDaysStr + ", getdate() ) , 23)";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                timestamp =  (rs.getString(1));
            }  
            else {
                return null;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain timestamp from DB", e );
            return null;
        }
        
        //We have to get the timestamp in "YYYY-MM-DD HH24:MM:SS" format, 
        //     the closest thing in sybase is format id 23, which has an embedded 'T'
        return timestamp.replace("T", " ");
    }
    
}
