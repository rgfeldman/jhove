/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.SQLException;
import java.util.logging.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;
import java.util.HashMap;



public class SiAssetMetaData {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    //class attributes
    String owningUnitUniqueName;
    String uoiid;  
    
    public HashMap <String,Integer> metaDataDBLengths; 

    Connection damsConn;
      
    public String getOwningUnitUniqueName() {
           return this.owningUnitUniqueName;
    }
    
    public String getUoiid() {
           return this.uoiid;
    }
    
    public void setOwningUnitUniqueName(String owningUnitUniqueName) {
           this.owningUnitUniqueName = owningUnitUniqueName;
    }
    
     
    public void setUoiid(String uoiid) {
           this.uoiid = uoiid;
    }
    
    
    /*  Method :        updateDAMSSourceSystemID
        Arguments:      
        Description:    
        RFeldman 2/2015
    */
    
    public int updateDAMSSourceSystemID (Connection damsConn, String uoiid, String sourceSystemId) {
        int recordsUpdated = 0;
        PreparedStatement pStmt = null;
        
        String sql = "update SI_ASSET_METADATA set source_system_id = '" + sourceSystemId + "' " +
                    "where UOI_ID = '" + uoiid + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
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
    
    public boolean populateMetaDataDBLengths (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        metaDataDBLengths = new HashMap<> (); 
        
        String sql = "SELECT column_name, data_length " + 
                     "FROM all_tab_columns " +
                     "WHERE table_name = 'SI_ASSET_METADATA' " + 
                     "AND owner = 'TOWNER' " +
                     "AND data_type != 'DATE' " + 
                     "AND column_name NOT IN ('PUBLIC_USE','UOI_ID','OWNING_UNIT_UNIQUE_NAME')";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs != null && rs.next()) {
                metaDataDBLengths.put(rs.getString(1),rs.getInt(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain data field lengths ", e);
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public boolean populateOwningUnitUniqueName (Connection damsConn) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT owning_unit_unique_name FROM si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setOwningUnitUniqueName (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return true;
    }
}
