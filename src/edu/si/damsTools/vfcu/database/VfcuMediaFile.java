/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.vfcu.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FilenameUtils;
import edu.si.damsTools.DamsTools;


public class VfcuMediaFile {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<Integer> filesIdsForBatch;
    private Integer maxFiles;
    private String  mediaFileName;
    private String  mediaFileDate;
    private String  mediaFileSize;
    private String  vendorChecksum;
    private String  vfcuChecksum;
    private Long    vfcuBatchNumber;
    private Integer vfcuMediaFileId;
    private Integer vfcuMd5FileId; 
    
    public ArrayList<Integer> getFilesIdsForBatch () {
        return this.filesIdsForBatch;
    }
    
        
    public String getMediaFileName () {
        return this.mediaFileName == null ? "" : this.mediaFileName;
    }
    
    public String getMediaFileDate () {
        return this.mediaFileDate;
    }
    
    public String getMediaFileSize () {
        return this.mediaFileSize;
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
    
    
 
    public void setMaxFiles (Integer maxFiles) {
        this.maxFiles = maxFiles;
    }
    
    public void setMediaFileName (String mediaFileName) {
        this.mediaFileName = mediaFileName;
    }
        
    public void setMediaFileDate (String mediaFileDate) {
        this.mediaFileDate = mediaFileDate;
    }
    
    public void setMediaFileSize (String mediaFileSize) {
        this.mediaFileSize = mediaFileSize;
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
    
   public boolean generateMediaFileId () {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            //Generate the ID for the primary key for this table
            String sql = "SELECT vfcu_media_file_id_seq.NextVal FROM dual"; 
            
            pStmt = DamsTools.getDamsConn().prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                this.vfcuMediaFileId = rs.getInt(1);
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to get identifier sequence for vfcu_md5_file", e );
                return false;
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (Exception se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (Exception se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public boolean insertRow () {
        
        String sql = "INSERT INTO vfcu_media_file ( " +
                        "vfcu_media_file_id, " +
                        "vfcu_md5_file_id, " +
                        "media_file_name, " +
                        "vendor_checksum) " +
                    "VALUES (" +
                        getVfcuMediaFileId() + ", " +
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
    
    public Integer countIncompleteFiles() {
        
        Integer numFiles = null;
        String sql =    "SELECT count(*) " +
                            "FROM vfcu_media_file a  " +
                            "WHERE a.vfcu_md5_file_id = " + getVfcuMd5FileId() +
                            " AND NOT EXISTS (" +
                            "   SELECT 'X' " +
                            "   FROM vfcu_activity_log b " +
                            "   WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                            "   AND b.vfcu_status_cd in ('PM','ER'))";
            
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
        
        String sql =    "SELECT count(*) " +
                            "FROM vfcu_media_file a " +
                            "WHERE a.vfcu_md5_file_id = " + getVfcuMd5FileId() + " " +
                            "AND EXISTS (" +
                            "   SELECT 'X' " +
                            "   FROM vfcu_activity_log b " +
                            "   WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                            "   AND b.vfcu_status_cd in ('PM','ER'))";
            
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
    
    public HashMap returnSuccessFileNmsIdForMd5Id () {
        
        HashMap<Integer, String> fileNameId;
        fileNameId = new HashMap<> ();
        
        String sql = "SELECT  vfcu_media_file_id, media_file_name " +
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
                 fileNameId.put(rs.getInt(1), rs.getString(2));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
        }
         
        return fileNameId;
    }
    
    public int returnCountStatusNoErrorForMd5Id (String status) {
        int mediaFileCount = 0;
        
        String sql = "SELECT count(*) " +
                     "FROM vfcu_media_file a " + 
                     "WHERE vfcu_md5_file_id = " + getVfcuMd5FileId() +
                     " AND EXISTS ( " +
                        "SELECT 'X' " +
                        "FROM vfcu_activity_log b " +
                        "WHERE a.vfcu_media_file_id = b.vfcu_media_file_id " +
                        "AND b.vfcu_status_cd = '" + status + "' )" +
                     " AND NOT EXISTS ( " +
                        "SELECT 'X' FROM vfcu_error_log c " +
                        "WHERE a.vfcu_media_file_id = c.vfcu_media_file_id ) " +
                     " AND NOT EXISTS ( " +
                        "SELECT 'X' " +
                        "FROM vfcu_activity_log d " +
                        "WHERE a.vfcu_media_file_id = d.vfcu_media_file_id " +
                        "AND d.vfcu_status_cd = 'ER' )";
          
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ) {
                       
            logger.log(Level.FINEST,"SQL! " + sql); 
           
            if (rs.next()) {
                mediaFileCount = (rs.getInt(1));
            }
            else {
                return 0;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to count number of media files in md5 file", e );
            return -1;
        }

        return mediaFileCount;
        
    }
    
    public int returnCountErrorForMd5Id () {
        int mediaFileCount = 0;
        
        String sql = "SELECT count(*) " +
                     "FROM vfcu_media_file a, " +
                     "     vfcu_error_log b " + 
                     "WHERE a.vfcu_md5_file_id = " + getVfcuMd5FileId() +
                     " AND a.vfcu_media_file_id = b.vfcu_media_file_id";
          
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery()  ) {
                       
            if (rs.next()) {
                mediaFileCount = (rs.getInt(1));
            }
            else {
                return 0;
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to count number of media files in md5 file", e );
            return -1;
        }

        return mediaFileCount;
        
    }
    
    
    public boolean populateFilesIdsForBatch () {
    
        filesIdsForBatch = new ArrayList<>();  
        String sql = "SELECT  vfcu_media_file_id " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_batch_number = " + getVfcuBatchNumber() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);
            
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            while (rs.next()) {
                 filesIdsForBatch.add(rs.getInt(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
                return false;
        }        
        return true;
    }
    
    public Integer returnIdForMd5IdBaseName() {
        
        String sql = "SELECT    vfcu_media_file_id " +
                     "FROM       vfcu_media_file a " +
                     "WHERE      vfcu_md5_file_id =  " + getVfcuMd5FileId() +
                     " AND       SUBSTR(media_file_name, 0, INSTR(media_file_name, '.')-1) = '" + FilenameUtils.getBaseName(getMediaFileName()) + "'";
                      
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                //found a matching filename
                return rs.getInt(1);
            }
            else {
                return 0;
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
                return -1;
        }
    }
    
    
    public HashMap retrieveSuccessFileNmsIdForMd5Id () {
        
        HashMap<Integer, String> fileNameId;
        fileNameId = new HashMap<> ();
        
        String sql = "SELECT  vfcu_media_file_id, media_file_name " +
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
                 fileNameId.put(rs.getInt(1), rs.getString(2));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain vfcu_media_file_ids for current batch", e );
        }
         
        return fileNameId;
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
    
      public boolean populateMd5FileId () {
    
        String sql = "SELECT  vfcu_md5_file_id " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setVfcuMd5FileId(rs.getInt(1));
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
                     "        media_file_size " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";         
        
        logger.log(Level.FINEST, "SQL: {0}", sql);    
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                 setMediaFileDate(rs.getString(1));
                 setMediaFileSize(rs.getString(2));
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
       
      
    public int updateVfcuBatchNumber () {
        
        int rowsUpdated = 0;
        
        //order by filename so each process has an even distribution of raws and .tifs.
        //Need multiple subqueries for order by with rownum clause
        String sql = 
            "UPDATE vfcu_media_file a " +
            "SET    a.vfcu_batch_number = " + getVfcuBatchNumber() + " " +
            "WHERE  a.vfcu_media_file_id IN ( " +
                "SELECT vfcu_media_file_id " + 
                "FROM ( " +
                    "Select * " +
                    "FROM vfcu_media_file b, " +
                    "     vfcu_md5_file c " +
                    "WHERE b.vfcu_md5_file_id = c.vfcu_md5_file_id " +
                    "AND   c.project_cd = '" + DamsTools.getProjectCd() + "' " +
                    "AND b.vfcu_batch_number IS NULL " +
                    "ORDER BY b.media_file_name ) " +
                    " where rownum < " + this.maxFiles + "+ 1) ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);     
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            rowsUpdated= pStmt.executeUpdate(); 
            
            logger.log(Level.FINER, "Rows updated for current batch" + rowsUpdated );
                
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
                     "          media_file_size = '" + getMediaFileSize() + "'" +
                     "WHERE     vfcu_media_file_id = " + getVfcuMediaFileId();
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql) ) {

            int rowsUpdated= pStmt.executeUpdate();        
            logger.log(Level.FINER, "Rows updated for current batch: " + rowsUpdated );
                    
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update checksum in vfcu_file_batch", e );
                return false;
        }
        
        return true;
    }
    
    public int retrieveSubFileId () {

        int childVfcuMediaId = 0;
        
        String sql = "SELECT  mediachild.vfcu_media_file_id " +
                     "FROM  vfcu_media_file mediamaster, " +
                     "      vfcu_md5_file b," +
                     "      vfcu_media_file mediachild " +
                     "WHERE mediamaster.vfcu_md5_file_id = b.MASTER_MD5_FILE_ID " +
                     "AND   mediachild.vfcu_md5_file_id = b.VFCU_MD5_FILE_ID " +
                     "AND   mediamaster.vfcu_md5_file_id != mediachild.vfcu_md5_file_id " +
                     "AND   SUBSTR(mediamaster.media_file_name, 0, INSTR(mediamaster.media_file_name, '.')-1) = " +
                     "SUBSTR(mediachild.media_file_name, 0, INSTR(mediachild.media_file_name, '.')-1) " +
                     "AND   mediamaster.vfcu_media_file_id = " + getVfcuMediaFileId(); 
                   
         logger.log(Level.FINEST,"SQL! " + sql); 
             
         try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
    
            if (rs.next()) {
                childVfcuMediaId = (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for child media ID in DB", e );
        }
         
        return childVfcuMediaId;
    }
    
}
