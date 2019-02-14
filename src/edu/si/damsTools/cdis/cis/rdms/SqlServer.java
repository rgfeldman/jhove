/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.rdms;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class SqlServer implements Rdms {
    
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
           
        String sql = "SELECT CONVERT(varchar, getdate()" + numDaysStr + ", 20)";        
        logger.log(Level.FINEST,"SQL! " + sql);
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
