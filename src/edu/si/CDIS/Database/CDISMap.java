/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database; 

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
import java.util.HashMap;

public class CDISMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer CDISMapId;
    Long batchNumber;
    String fileName;
    String uoiid;
    String cisUniqueMediaId;
    Integer vfcuMediaFileId;
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
    
    public String getDamsUoiid () {
        return this.uoiid;
    }
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
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
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
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
    
     public boolean populateIdFromVfcuId (Connection damsConn) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE vfcu_media_file_id = " + getVfcuMediaFileId() +
                    " AND batch_number = " + getBatchNumber();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                throw new Exception();
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
    
    
    public boolean createRecord(CDIS cdis) {
        
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
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        try {
            
            sql =  "INSERT INTO cdis_map (" +
                            "cdis_map_id, " +
                            "si_holding_unit, " +
                            "file_name, " +
                            "batch_number, " +
                            "vfcu_media_file_id, " +
                            "deleted_ind, " +
                            "error_ind ) " +
                        "VALUES (" +
                            getCdisMapId() + ", " +
                            "'" + cdis.properties.getProperty("siHoldingUnit") + "', " +
                            "'" + getFileName() + "', " +
                            cdis.getBatchNumber() + ", " +
                            getVfcuMediaFileId() + ", " +
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
                      "SET dams_uoi_id = '" + getDamsUoiid() + "' " +
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
    
    public HashMap<Integer, String> findNullUoiids (Connection dbConn) {
        
        HashMap<Integer, String> unlinkedDamsRecords;
        unlinkedDamsRecords = new HashMap<> ();
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id, file_name " +
                    "FROM CDIS " +
                    "WHERE dams_uoi_id IS NULL ";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = dbConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                // NEED TO DO THIS
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/batch", e );
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return unlinkedDamsRecords;
        
    }
    
}
