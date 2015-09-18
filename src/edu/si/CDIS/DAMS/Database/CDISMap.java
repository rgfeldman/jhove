/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

/**
 *
 * @author rfeldman
 */
import edu.si.CDIS.CDIS;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;

public class CDISMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer CDISMapId;
    Long batchNumber;
    String fileName;
    String uoiid;
    String cisUniqueMediaId;
    char errorInd;
    
    public Long getBatchNumber () {
        return this.batchNumber;
    }
       
    public Integer getCdisMapId () {
        return this.CDISMapId;
    }
    
    public String getCisUniqueMediaId () {
        return this.cisUniqueMediaId;
    }
    
    public char getErrorInd () {
        return this.errorInd;
    }
    
    public String getFileName () {
        return this.fileName;
    }
    
    public String getUoiid () {
        return this.uoiid;
    }
    
    public void setBatchNumber (Long batchNumber) {
        this.batchNumber = batchNumber;
    }
        
    public void setCdisMapId (Integer CDISMapId) {
        this.CDISMapId = CDISMapId;
    }
    
    public void setErrorInd (char errorInd) {
        this.errorInd = errorInd;
    }
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    public void setCisUniqueMediaId (String cisUniqueMediaId) {
        this.cisUniqueMediaId = cisUniqueMediaId;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
    
   
    public boolean populateMapInfo (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cis_unique_media_id, " + 
                            "dams_uoi_id, " +
                            "file_name " +
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setCisUniqueMediaId (rs.getString(1));
                setUoiid (rs.getString(2));
                setFileName (rs.getString(3));
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
    
    public boolean populateIDForFileBatch (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "'" +
                    "AND batch_number = " + getBatchNumber();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/batch", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public boolean createRecord(CDIS cdis, String cisUniqueMediaId, String cisFileName) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        int rowsUpdated = 0;
  
        String sql = "SELECT cdis_map_id_seq.NextVal from dual";
        
         try {
            
            pStmt = cdis.damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();

            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }    
            
        
            sql =  "INSERT INTO cdis_map (" +
                            "cdis_map_id, " +
                            "si_holding_unit, " +
                            "cis_unique_media_id, " +
                            "file_name, " +
                            "batch_number, " +
                            "deleted_ind, " +
                            "error_ind ) " +
                        "VALUES (" +
                            getCdisMapId() + ", " +
                            "'" + cdis.properties.getProperty("siHoldingUnit") + "', " +
                            "'" + cisUniqueMediaId + "', " +
                            "'" + cisFileName + "', " +
                            cdis.getBatchNumber() + ", " +
                            "'N' ," +
                            "'N')";
                 
            logger.log(Level.FINEST,"SQL! " + sql);  
        
            pStmt = cdis.damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
                
        return true;
       
    }
    
    public boolean updateUoiid(Connection damsConn) {
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET dams_uoi_id = '" + getUoiid() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with uoi_id", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }   
        
        return true;
    }
    
    public boolean updateErrorInd (Connection damsConn) {
        
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET error_ind = '" + getErrorInd() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with Error indicator", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }   
        
        return true;
    }
    
}
