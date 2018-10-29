/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database; 

/**
 *
 * @author rfeldman
 */
import edu.si.damsTools.DamsTools;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CdisMap {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer cdisMapId;
    private String fileName;
    private String damsUoiid;
    private Integer vfcuMediaFileId;
    private char errorInd;
    
    public Integer getCdisMapId () {
        return this.cdisMapId;
    }
    
    public String getDamsUoiid () {
         return this.damsUoiid == null ? "" : this.damsUoiid;
    }
    
    public char getErrorInd () {
        return this.errorInd;
    }
    
    public String getFileName () {
        return this.fileName == null ? "" : this.fileName;
    }
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
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
    
    public void setDamsUoiid (String damsUoiid) {
        this.damsUoiid = damsUoiid;
    }
        
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
   
    public boolean createRecord() {
        
        int rowsUpdated = 0; 
        String sql = "SELECT cdis_map_id_seq.NextVal from dual";
        
         try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery()) {

            if (rs != null && rs.next()) {
                setCdisMapId (rs.getInt(1));
            }    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into CDIS_MAP table", e );
                return false;
        }
          
        sql =  "INSERT INTO cdis_map (" +
                    "cdis_map_id, " +
                    "project_cd, " +
                    "dams_uoi_id, " +
                    "file_name, " +
                    "batch_number, " +
                    "vfcu_media_file_id ) " +
                "VALUES (" +
                    getCdisMapId() + ", " +
                    "'" + DamsTools.getProjectCd() + "', " +
                    "'" + getDamsUoiid() + "', " +
                    "'" + getFileName() + "', " +
                    DamsTools.getBatchNumber() + ", " +
                    getVfcuMediaFileId() + ")";
                 
        logger.log(Level.FINEST,"SQL! " + sql);      
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
        
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
    
    public boolean populateIdForNameNullUoiid () {

        String sql = "SELECT cdis_map_id FROM cdis_map " +
                    "WHERE file_name = '" + getFileName() + "' " +
                    "AND project_cd = '" + DamsTools.getProjectCd() + "' " +
                    "AND dams_uoi_id IS NULL ";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
                    " AND batch_number = " + DamsTools.getBatchNumber();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
                    "AND batch_number = " + DamsTools.getBatchNumber();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
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
        
        String sql = "SELECT dams_uoi_id, " +
                            "file_name " +
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()){                   
            
            if (rs != null && rs.next()) {
                setDamsUoiid (rs.getString(1));
                setFileName (rs.getString(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
        return true;
    }
    
    public boolean populateVfcuMediaFileId () {

        String sql = "SELECT vfcu_media_file_id " + 
                    "FROM cdis_map " +
                    "WHERE cdis_map_id = " + getCdisMapId();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                setVfcuMediaFileId (rs.getInt(1));
                return true;
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        }
    }
    
    public boolean populateParentFileInfo (Integer relatedMapId) {
        
        //Get the pattern of the parent file
        String sql = "SELECT REGEXP_REPLACE (a.file_name, " + 
                    "b.filename_pattern, b.parent_filename_pattern) " +
                    "FROM cdis_map a, " +
                    "   media_type_config_r b " +
                    "WHERE  a.media_type_config_id = b.media_type_config_id " +
                    "AND    a.project_cd = '" + DamsTools.getProjectCd() + "' " +
                    "AND    a.cdis_map_id = " + relatedMapId;
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        String parentPattern = null;
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                parentPattern = rs.getString(1);
            }   
            else {
                logger.log(Level.FINER, "No parent found");
                return false;
            }
            
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain parent pattern info from cdis_map", e );
            return false;
        }
        
        
        //Find the parent file matching that pattern
        sql =  "SELECT cdis_map_id, file_name, dams_uoi_id " +
              "FROM cdis_map " + 
              "WHERE REGEXP_LIKE (file_name, " +
              " '(^" + parentPattern + "*)', 'i')" +
              "AND dams_uoi_id IS NOT NULL " +  
              "AND project_cd = '" + DamsTools.getProjectCd() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                setCdisMapId(rs.getInt(1));
                setFileName(rs.getString(2));
                setDamsUoiid(rs.getString(3));
            }   
            else {
                logger.log(Level.FINER, "No record found");
                return false;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain parent info from cdis_map", e );
            return false;
        }
        
        return true;
    }
    
    public boolean populateChldFileInfo (Integer relatedMapId) {
        
        //Get the pattern of the parent file
        
        String sql = "SELECT REGEXP_REPLACE (a.file_name, " + 
                    "b.filename_pattern, b.child_filename_pattern) " +
                    "FROM cdis_map a, " +
                    "   media_type_config_r b " +
                    "WHERE  a.media_type_config_id = b.media_type_config_id " +
                    "AND    a.project_cd = '" + DamsTools.getProjectCd() + "' " +
                    "AND    a.cdis_map_id = " + relatedMapId;
        
        logger.log(Level.FINEST,"SQL! " + sql);     
  
        String childPattern = null;
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                childPattern = rs.getString(1);
            }   
            else {
                logger.log(Level.FINER, "No child found");
                return false;
            }
            
        } catch (Exception e) {
                 logger.log(Level.FINER, "Error: unable to obtain child pattern info from cdis_map", e );
                return false;
        }
        
        //Find the parent file matching that pattern
       sql =  "SELECT cdis_map_id, file_name, dams_uoi_id " +
              "FROM cdis_map " + 
              "WHERE REGEXP_LIKE (file_name, " +
              " '(^" + childPattern + "*)', 'i') " +
              "AND dams_uoi_id IS NOT NULL " +
              "AND project_cd = '" + DamsTools.getProjectCd() + "'";
        
       logger.log(Level.FINEST,"SQL! " + sql); 
       try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()) { 
            
            if (rs != null && rs.next()) {
                setCdisMapId(rs.getInt(1));
                setFileName(rs.getString(2));
                setDamsUoiid(rs.getString(3));
            }    
            else {
                logger.log(Level.FINER, "No record found");
                return false;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to obtain child info from cdis_map", e );
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
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            
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
    
}
