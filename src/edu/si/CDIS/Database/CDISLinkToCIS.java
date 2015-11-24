/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
        
public class CDISLinkToCIS {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    String cisUniqueMediaId;
    String cisChecksum;
    
    public ArrayList getReadyToIntegrateRecords (Connection dbConn) {
        
        ArrayList<String> cisIdsToIntegrate = new ArrayList<>();
         
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT    cis_unique_media_id " + 
                    "FROM       cdis_link_to_cis " +
                    "WHERE      cdis_ingest_status_cd = 'RI' ";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = dbConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs.next()) {
                cisIdsToIntegrate.add(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return cisIdsToIntegrate;
        
    }
    
}
