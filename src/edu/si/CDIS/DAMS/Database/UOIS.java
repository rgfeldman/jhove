/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class UOIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String uoiid;
    
    
    public String getUoiid () {
        return this.uoiid;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
    
    
    
    /*  Method :        updateMetaDataStateDate
        Arguments:      
        Returns:      
        Description:    Update the UOIS table with the MetaDataStateDate.  This will trigger IDS.
        RFeldman 2/2015
    */
    public Integer updateMetaDataStateDate(Connection damsConn) {
        
        int recordsUpdated = 0;
        PreparedStatement pStmt = null;
        
        // We have not met any of the above conditions, we should update for IDS
        String sql = "UPDATE uois " +
                    "SET metadata_state_dt = SYSDATE, " +
                    "    metadata_state_user_id = '22246' " +
                    "WHERE uoi_id = '" + this.uoiid + "'";
        
        logger.log(Level.ALL, "updateUOIIS Statment: " + sql);
        
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            recordsUpdated = pStmt.executeUpdate(sql);
            
        
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
            
        return recordsUpdated;

    }
}
