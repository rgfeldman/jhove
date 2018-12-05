/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.utilities.StringUtils;



public class VfcuMediaFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
     
    private Integer maxFiles;
    private String  mediaFileName;
    private String  mediaFileDate;
    private Double  mbFileSize;
    private String  vendorChecksum;
    private String  vfcuChecksum;
    private Long    vfcuBatchNumber;
    private Integer vfcuMediaFileId;
    private Integer vfcuMd5FileId; 
    private Integer childVfcuMediaFileId;
        
    public Integer getChildVfcuMediaFileId () {
        return this.childVfcuMediaFileId;
    }
      
    public String getMediaFileName () {
        return this.mediaFileName == null ? "" : this.mediaFileName;
    }
    
    public String getMediaFileDate () {
        return this.mediaFileDate;
    }
    
    public Double getMbFileSize () {
        return this.mbFileSize;
    }
    
    public Integer getVfcuMd5FileId () {
        return this.vfcuMd5FileId;
    }
    
    public String getVendorChecksum () {
        return this.vendorChecksum;
    }
    
    public String getVfcuChecksum () {
        return this.vfcuChecksum;
    }
    
    public Long getVfcuBatchNumber () {
        return this.vfcuBatchNumber;
    }
    
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    
    public void setChildVfcuMediaFileId (Integer childVfcuMediaFileId) {
        this.childVfcuMediaFileId = childVfcuMediaFileId;
    }
        
    public void setMaxFiles (Integer maxFiles) {
        this.maxFiles = maxFiles;
    }
    
    public void setMediaFileName (String mediaFileName) {
        this.mediaFileName = mediaFileName;
    }
        
    public void setMediaFileDate (String mediaFileDate) {
        this.mediaFileDate = mediaFileDate;
    }
    
    public void setMbFileSize (Double mbFileSize) {
        this.mbFileSize = mbFileSize;
    }
        
    public void setVendorCheckSum (String vendorCheckSum) {
        this.vendorChecksum = vendorCheckSum;
    }
    
    public void setVfcuBatchNumber (Long vfcuBatchNumber) {
        this.vfcuBatchNumber = vfcuBatchNumber;
    }
    
    public void setVfcuChecksum (String vfcuChecksum) {
        this.vfcuChecksum = vfcuChecksum;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
    }
    
    
    public void setVfcuMd5FileId (Integer vfcuMd5FileId) {
        this.vfcuMd5FileId = vfcuMd5FileId;
    }
    
    public boolean insertRow () {
        
        String sql = "INSERT INTO vfcu_media_file ( " +
                        "vfcu_media_file_id, " +
                        "vfcu_md5_file_id, " +
                        "media_file_name, " +
                        "vendor_checksum) " +
                    "VALUES (" +
                        "vfcu_media_file_id_seq.NextVal, " + 
                        getVfcuMd5FileId() + ", " +
                        "'" + getMediaFileName() + "'," +
                        "'" + getVendorChecksum() + "')";
    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) )  {
            logger.log(Level.FINEST, "SQL: {0}", sql);

            int rowsInserted = pStmt.executeUpdate(); 
            
            if (rowsInserted != 1) {
                throw new Exception();
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert into vfcu_media_file", e );
                return false;
        }
        return true;
    }
    
    public int returnCountFilesForMd5Id () {
        int numFiles = 0; 
        
        String sql = "SELECT  count(*) " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_md5_file_id = " + getVfcuMd5FileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);         
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 numFiles = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return -1;
        }
        return numFiles;
    }
    
    
    public Integer getNumCompleteFilesForMd5FileId () {
        Integer numFiles = 0;
        
        String completedStatus = "PM";
        if (DamsTools.getProperty("useMasterSubPairs").equals("true")) {
            completedStatus = "PS";
        }
              
        String sql =    "SELECT count(*) " +
                        "FROM vfcu_media_file vmf " +
                        "WHERE vmf.vfcu_md5_file_id = " + getVfcuMd5FileId() +
                        " AND EXISTS ( " +
                            "SELECT 'X' FROM vfcu_activity_log val " +
                            "WHERE val.vfcu_media_file_id = vmf.vfcu_media_file_id " +
                            "AND val.vfcu_status_cd in ('" + completedStatus + "','ER'))";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
           
            while (rs.next()) {
                 numFiles = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
                return -1;
        }
        
        return numFiles;
    }
    
     public Integer returnAssociatedFileId () {
        Integer assocVfcuFileId = null;
        
        String sql = "SELECT    subvmf.vfcu_media_file_id " +
                     "FROM      vfcu_media_file vmf " +
                     "INNER JOIN vfcu_md5_file vmd " +
                     "ON vmf.vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                     "INNER JOIN vfcu_md5_file_hierarchy vmfh " + 
                     "ON vmd.vfcu_md5_file_id = vmfh.masterfile_vfcu_md5_file_id " +
                     "INNER JOIN vfcu_media_file subvmf " +
                     "ON subvmf.vfcu_md5_File_id = vmfh.subfile_vfcu_md5_file_id " +
                     "WHERE vmf.vfcu_media_file_id = " + getVfcuMediaFileId() +
                     " AND SUBSTR(subvmf.media_file_name,1,INSTR(subvmf.media_file_name,'.', -1) -1)  = "
                            + "'" + StringUtils.getExtensionlessFileName(getMediaFileName()) +"'" ;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
                    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
           if (rs.next()) {
                //found a matching filename
                assocVfcuFileId = rs.getInt(1);
            }

        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to return Associated File Id", e );
        }
        
        return assocVfcuFileId;
    }
    
    
    public ArrayList<Integer> returnVfcuFileIdsForBatch () {
    
        ArrayList<Integer> filesIdsForBatch = new ArrayList<>();  
        String sql = "SELECT    vfcu_media_file_id " +
                     "FROM      vfcu_media_file " +
                     "WHERE     vfcu_batch_number = " + getVfcuBatchNumber() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs.next()) {
                 filesIdsForBatch.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
        }        
        return filesIdsForBatch;
    }
    
     public Integer returnIdForNameOtherMd5 () {
        
        Integer otherFile_id = null;
        
        String sql = "SELECT  vfcu_media_file_id " +
                     "FROM    vfcu_media_file vmf " +
                     "INNER JOIN vfcu_md5_file md5 " +
                     "ON      vmf.vfcu_md5_file_id = md5.vfcu_md5_file_id " +
                     "WHERE   vmf.vfcu_md5_file_id != " + getVfcuMd5FileId() +
                     " AND    media_file_name = '" + getMediaFileName() + "' " +
                     "AND    md5.project_cd = '" + DamsTools.getProjectCd() + "' " +
                     "AND NOT EXISTS ( " +
                        "SELECT 'X' FROM vfcu_error_log vel  " +
                        "WHERE vmf.vfcu_media_file_id = vel.vfcu_media_file_id) ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);         
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
           if (rs.next()) {
                //found a matching filename
                otherFile_id = rs.getInt(1);
            }

        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for duplicate fileName", e );
        }
        
        return otherFile_id;
    }
    
    
    public ArrayList retrieveNoErrorIdsForMd5Id () {
        
        ArrayList<Integer> ids = new ArrayList<>();
        
        String sql = "SELECT  vfcu_media_file_id " +
                     "FROM     vfcu_media_file a " +
                     "WHERE    vfcu_md5_file_id = " + getVfcuMd5FileId() + 
                     " AND NOT EXISTS ( " +
                     "  SELECT 'X' " +
                     " FROM vfcu_error_log b " +
                     " WHERE a.vfcu_media_file_id = b.vfcu_media_file_id) ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            while (rs.next()) {
                 ids.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
        }
         
        return ids;
    }
    
    public boolean populateVendorChecksum () {
    
        String sql = "SELECT  vendor_checksum " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 setVendorCheckSum(rs.getString(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return false;
        }
         
        return true;
    }

     public boolean populateMediaFileName () {
    
        String sql = "SELECT  media_file_name " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setMediaFileName(rs.getString(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get Media FileName", e );
                return false;
        }
        return true;
    }
    
     public boolean populateBasicDbData () {
    
        String sql = "SELECT  vfcu_md5_file_id, media_file_name, vendor_checksum, child_vfcu_media_file_id, mb_file_size " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setVfcuMd5FileId(rs.getInt(1));
                 setMediaFileName(rs.getString(2));
                 setVendorCheckSum(rs.getString(3));
                 setChildVfcuMediaFileId(rs.getInt(4));       
            } 
            else {
                logger.log(Level.FINER, "unable get Media FileName");
                return false;
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get Media FileName", e );
                return false;
        } 
        return true;
    }
      
    public boolean populateMediaFileAttr () {
    
        String sql = "SELECT  TO_CHAR(media_file_date,'YYYY-MM-DD'), " +
                     "        mb_file_size " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";         
        
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setMediaFileDate(rs.getString(1));
                 setMbFileSize(rs.getDouble(2));
            }
            else {
                return false;
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get Media FileDate", e );
                return false;
        }
         
        return true;
    }
    
    public boolean populateMediaFileIdForFileNameMd5Id () {
        
        String sql = "SELECT  vfcu_media_file_id " +
                     "FROM    vfcu_media_file " +
                     "WHERE   media_file_name = '" + getMediaFileName() + "'" +
                     "AND     vfcu_md5_file_id = " + getVfcuMd5FileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setVfcuMd5FileId(rs.getInt(1));
            }
            else {
                return false;
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get Media FileName", e );
                return false;
        } 
        return true;
    }
    
    public int returnCountForMd5FileId() {
        
        int totalCount = 0;
        
        String sql = "SELECT count(*) " +
                     "FROM vfcu_media_file vmf " +
                     "WHERE vfcu_md5_file_id = " + getVfcuMd5FileId() ;
                     
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                totalCount = rs.getInt(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to get count of completed records", e );
        }

        return totalCount;
        
    }
       
      
    public int updatePickupValidateBatch () {
        
        int rowsUpdated = 0;
        
        //order by md5 file so if we have a lot of files to process from a lot of folders, 
        //at least we can see some completing before too long
        //Need multiple subqueries for order by with rownum clause
        String sql = 
            "UPDATE vfcu_media_file vmf " +
            "SET    vmf.vfcu_batch_number = " + getVfcuBatchNumber() + " " +
            "WHERE  vmf.vfcu_media_file_id IN ( " +
                "SELECT vfcu_media_file_id " + 
                "FROM ( " +
                    "SELECT * " +
                    "FROM vfcu_media_file vmf2 " +
                    "INNER JOIN vfcu_md5_file vmd " +
                    "ON vmf2.vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                    "INNER JOIN vfcu_md5_file_hierarchy vmdh " +
                    "ON vmdh.masterFile_vfcu_md5_file_id = vmd.vfcu_md5_file_id " +
                    "AND vmd.project_cd = '" + DamsTools.getProjectCd() + "' " +
                    "AND vmf2.vfcu_batch_number IS NULL " +
                    "ORDER BY vmf2.vfcu_md5_file_id ) " +
                "WHERE ROWNUM < " + this.maxFiles + "+ 1) ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated for current batch: " + rowsUpdated );
                
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update batch number in vfcu_file_batch", e );
            return -1;
        }
        return rowsUpdated;
    }
    
    
    public boolean updateVfcuMediaAttributes () {
 
        String sql = "UPDATE    vfcu_media_file " +
                     "SET       vfcu_checksum = LOWER('" + getVfcuChecksum() + "'), " +
                     "          media_file_date = TO_DATE ('" + getMediaFileDate() + "','YYYY-MM-DD'), " +
                     "          mb_file_size = '" + getMbFileSize() + "' " +
                     "WHERE     vfcu_media_file_id = " + getVfcuMediaFileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            int rowsUpdated= pStmt.executeUpdate();        
            logger.log(Level.FINER, "Rows updated: " + rowsUpdated );
                    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update checksum in vfcu_file_batch", e );
                return false;
        }
        
        return true;
    }
    
    public boolean updateChildVfcuMediaFileId () {
 
        String sql = "UPDATE    vfcu_media_file " +
                     "SET       child_vfcu_media_file_id = " + getChildVfcuMediaFileId() +
                     " WHERE     vfcu_media_file_id = " + getVfcuMediaFileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            int rowsUpdated= pStmt.executeUpdate();        
            logger.log(Level.FINER, "Rows updated: " + rowsUpdated );
                    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update child vfcuMediaFile_id", e );
                return false;
        }
        
        return true;
    }
    
     public boolean updateVfcuBatchNumber () {
 
        String sql = "UPDATE    vfcu_media_file " +
                     "SET       vfcu_batch_number = '" + DamsTools.getBatchNumber() + "' " +
                     " WHERE     vfcu_media_file_id = " + getVfcuMediaFileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            int rowsUpdated= pStmt.executeUpdate();        
            logger.log(Level.FINER, "Rows updated: " + rowsUpdated );
                    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update child vfcuMediaFile_id", e );
                return false;
        }
        
        return true;
    }
    
    public Integer retrieveParentVfcuMediafileId() {
        
        Integer subfileId = null;
        
        String sql = "SELECT  vfcu_media_file_id " +
                     "FROM    vfcu_media_file a " +
                     "WHERE   child_vfcu_media_file_id = " + this.vfcuMediaFileId;
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 subfileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain Master File Id for child", e );
        }
         
        return subfileId;
    }
    
    public Integer retrieveSubFileId() {
        
        Integer subfileId = null;
        
        String sql = "SELECT  child_vfcu_media_file_id " +
                     "FROM    vfcu_media_file a " +
                     "WHERE   vfcu_media_file_id = " + getVfcuMediaFileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 subfileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain child Record Id", e );
        }
         
        return subfileId;
    }
}
