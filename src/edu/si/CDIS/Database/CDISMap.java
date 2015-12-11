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
import java.util.HashMap;

public class CDISMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer CDISMapId;
    private Long batchNumber;
    private String fileName;
    private String damsUoiid;
    private String cisUniqueMediaId;
    private Integer vfcuMediaFileId;
    private char errorInd;
    
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
        return this.damsUoiid;
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
    
    public void setDamsUoiid (String damsUoiid) {
        this.damsUoiid = damsUoiid;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
   
    public boolean createRecord() {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        int rowsUpdated = 0;
  
        String sql = "SELECT cdis_map_id_seq.NextVal from dual";
        
         try {
            
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
                            "'" + CDIS.getProperty("siHoldingUnit") + "', " +
                            "'" + getFileName() + "', " +
                            CDIS.getBatchNumber() + ", " +
                            getVfcuMediaFileId() + ", " +
                            "'N' ," +
                            "'N')";
                 
            logger.log(Level.FINEST,"SQL! " + sql);  
        
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
    
    
    public boolean populateIdForNameNullUoiid () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "' " +
                    "AND dams_uoi_id IS NULL " +
                    "AND error_ind is 'N' " + 
                    "AND deleted_ind is 'N' ";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/null uoiid", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }     
        return true; 
    }
    
    
    public boolean populateIdFromVfcuId () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE vfcu_media_file_id = " + getVfcuMediaFileId() +
                    " AND batch_number = " + getBatchNumber();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
    
    public boolean populateIDForFileBatch () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "'" +
                    "AND batch_number = " + getBatchNumber();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
    
    public boolean populateMapInfo () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT cis_unique_media_id, " + 
                            "dams_uoi_id, " +
                            "file_name " +
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setCisUniqueMediaId (rs.getString(1));
                setDamsUoiid (rs.getString(2));
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
    
    public boolean populateVfcuId () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT vfcu_media_file_id " + 
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setVfcuMediaFileId (rs.getInt(1));
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
    
    
    
    public boolean updateCisUniqueMediaId() {
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rowsUpdated = pStmt.executeUpdate(sql);
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with cis_unique_media_id", e );
                return false;
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }   
        
        return true;
    }
    
    
    public boolean updateUoiid() {
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET dams_uoi_id = '" + getDamsUoiid() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
    
    public boolean updateErrorInd () {
        
        PreparedStatement pStmt = null;
        int rowsUpdated = 0;
        
        String sql =  "UPDATE cdis_map " +
                      "SET error_ind = '" + getErrorInd() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try {
            
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
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
    
    public HashMap<Integer, String> returnUnlinkedMediaInDams () {
        
        HashMap<Integer, String> unlinkedDamsRecords;
        unlinkedDamsRecords = new HashMap<> ();
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT    a.cdis_map_id, a.file_name " +
                    "FROM       cdis_map a, " +
                    "           cdis_activity_log b, " +
                    "           towner.uois c " +
                    "WHERE      a.cdis_map_id = b.cdis_map_id " +
                    "AND        a.file_name = c.name " +
                    "AND        b.cdis_status_cd IN ('FCS', 'FMM') " +
                    "AND        dams_uoi_id IS NULL " +
                    "AND        a.error_ind = 'N' " +
                    "AND        a.deleted_ind = 'N'";
                                
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = CDIS.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            while (rs.next()) {
                unlinkedDamsRecords.put(rs.getInt(1), rs.getString(2));
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
