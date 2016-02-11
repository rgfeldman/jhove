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
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;

public class CDISMap {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer cdisMapId;
    private Integer cdisCisMediaTypeId;
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
        return this.cdisMapId;
    }
    
    public Integer getCdisCisMediaTypeId () {
        return this.cdisCisMediaTypeId;
    }
    
    public String getCisUniqueMediaId () {
        return this.cisUniqueMediaId == null ? "" : this.cisUniqueMediaId;
    }
    
    public char getErrorInd () {
        return this.errorInd;
    }
    
    public String getFileName () {
        return this.fileName == null ? "" : this.fileName;
    }
    
    public String getDamsUoiid () {
         return this.damsUoiid == null ? "" : this.damsUoiid;
    }
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    
    public void setBatchNumber (Long batchNumber) {
        this.batchNumber = batchNumber;
    }
        
    public void setCdisMapId (Integer cdisMapId) {
        this.cdisMapId = cdisMapId;
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
    
    public void setCdisCisMediaTypeId (Integer cdisCisMediaTypeId) {
        this.cdisCisMediaTypeId = cdisCisMediaTypeId;
    }
    
    public void setDamsUoiid (String damsUoiid) {
        this.damsUoiid = damsUoiid;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
   
    public boolean createRecord() {
        
        int rowsUpdated = 0; 
        String sql = "SELECT cdis_map_id_seq.NextVal from dual";
        
         try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery()) {

            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }
         
        if (getDamsUoiid() == null) {
        
        }
          
        sql =  "INSERT INTO cdis_map (" +
                    "cdis_map_id, " +
                    "collection_group_cd, " +
                    "cis_unique_media_id, " +
                    "dams_uoi_id, " +
                    "file_name, " +
                    "batch_number, " +
                    "vfcu_media_file_id, " +
                    "cdis_cis_media_type_id) " +
                "VALUES (" +
                    getCdisMapId() + ", " +
                    "'" + CDIS.getProperty("collectionGroup") + "', " +
                    "'" + getCisUniqueMediaId() + "', " +
                    "'" + getDamsUoiid() + "', " +
                    "'" + getFileName() + "', " +
                    CDIS.getBatchNumber() + ", " +
                    getVfcuMediaFileId() + ", " +
                    getCdisCisMediaTypeId() + ")";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
        
            rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }      
        return true;
    }
    
    public int getMasterIdFromChildId () {
        
        int masterMapId = 0;
        
        String sql = "SELECT	cdismaster.cdis_map_id " +
                    "FROM       cdis_map cdischild, " +
                    "           vfcu_media_file vfcuchild, " +
                    "           vfcu_md5_file c, " +
                    "           vfcu_media_file vfcumaster, " +
                    "           cdis_map cdismaster " +
                    "WHERE      cdischild.vfcu_media_file_id = vfcuchild.vfcu_media_file_id " +
                    "AND        vfcuchild.vfcu_md5_file_id = c.vfcu_md5_file_id " +
                    "AND        c.master_md5_file_id =  vfcumaster.vfcu_md5_file_id " +
                    "AND        vfcumaster.vfcu_media_file_id = cdismaster.vfcu_media_file_id " +
                    "AND        cdismaster.cdis_map_id != cdischild.cdis_map_id " +
                    "AND        vfcumaster.vfcu_md5_file_id ! = vfcuchild.vfcu_md5_file_id " +
                    "AND        SUBSTR(vfcumaster.media_file_name,1,LENGTH(vfcumaster.media_file_name) -3) = " +
                    "           SUBSTR(vfcuchild.media_file_name,1,length(vfcuchild.media_file_name) -3) " +
                    "AND        SUBSTR(cdismaster.file_name,1,LENGTH(cdismaster.file_name) -3) = " + 
                    "           SUBSTR(cdischild.file_name,1,LENGTH(cdischild.file_name) -3) " +
                    "AND        cdischild.cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ){
 
            if (rs != null && rs.next()) {
                masterMapId = rs.getInt(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/null uoiid", e );
        }
        
        return masterMapId;
 
    }
    
     public boolean populateIdFromCisMediaId () {

        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                    "AND collection_group_cd = '" + CDIS.getProperty("collectionGroup") + "' " +
                    "AND to_history_dt is NULL ";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for cis ID", e );
                return false;
        }
        return true;
    }
     
    public boolean populateIdForNameNullUoiid () {

        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "' " +
                    "AND dams_uoi_id IS NULL " +
                    "AND to_history_dt IS NULL";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ){
 
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/null uoiid", e );
                return false;
        }
        return true; 
    }
    
    public boolean populateIdFromUoiid () {

        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE dams_uoi_id = '" + getDamsUoiid() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for uoiid", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIdFromVfcuId () {

        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE vfcu_media_file_id = " + getVfcuMediaFileId() +
                    " AND batch_number = " + getBatchNumber();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
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
        }
        return true;
    }
    
    public boolean populateIDForFileBatch () {
  
        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "'" +
                    "AND batch_number = " + getBatchNumber();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/batch", e );
                return false;
        }
        return true;
    }
    
    public boolean populateMapInfo () {
        
        String sql = "SELECT cis_unique_media_id, " + 
                            "dams_uoi_id, " +
                            "file_name " +
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()){                   
            
            if (rs != null && rs.next()) {
                setCisUniqueMediaId (rs.getString(1));
                setDamsUoiid (rs.getString(2));
                setFileName (rs.getString(3));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    public boolean populateVfcuId () {

        String sql = "SELECT vfcu_media_file_id " + 
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                setVfcuMediaFileId (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    
    public boolean updateCisUniqueMediaId() {
        
        int rowsUpdated = 0;
        String sql =  "UPDATE cdis_map " +
                      "SET cis_unique_media_id = '" + getCisUniqueMediaId() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql);     
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            
            rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with cis_unique_media_id", e );
                return false;
        } 
        return true;
    }
    
    
    public boolean updateUoiid() {
        
        int rowsUpdated = 0;
        String sql =  "UPDATE cdis_map " +
                      "SET dams_uoi_id = '" + getDamsUoiid() + "' " +
                      "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql)) {
            
            rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update CDIS_MAP table with uoi_id", e );
                return false;
        }
        return true;
    }
    
    public HashMap<Integer, String> returnUnlinkedMediaInDams () {
        
        HashMap<Integer, String> unlinkedDamsRecords;
        unlinkedDamsRecords = new HashMap<> ();
        String sql = "SELECT    a.cdis_map_id, a.file_name " +
                    "FROM       cdis_map a, " +
                    "           cdis_activity_log b, " +
                    "           towner.uois c " +
                    "WHERE      a.cdis_map_id = b.cdis_map_id " +
                    "AND        a.file_name = c.name " +
                    "AND        a.dams_uoi_id IS NULL " +
                    "AND        a.to_history_dt IS NULL " + 
                    "AND        a.collection_group_cd = '" + CDIS.getProperty("collectionGroup") + "' " + 
                    "AND        b.cdis_status_cd IN ('FCS', 'FMM') " +
                    "AND        c.content_state = 'NORMAL' " +
                    "AND        c.content_type != 'SHORTCUT' " +
                    "AND NOT EXISTS ( " + 
                        "SELECT 'X' " +
                        "FROM cdis_error d " +
                        "WHERE a.cdis_map_id = d.CDIS_MAP_ID)" ;
                
                                
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
              ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs.next()) {
                unlinkedDamsRecords.put(rs.getInt(1), rs.getString(2));
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain map_id for file/batch", e );
        }
        return unlinkedDamsRecords; 
    }
}
