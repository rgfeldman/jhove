/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VFCUMediaFile {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    private Integer vfcuMediaFileId;
    private String  mediaFileName;
    private String vendorChecksum;
    
    public String getMediaFileName () {
        return this.mediaFileName;
    }
    
    public String getVendorChecksum () {
        return this.vendorChecksum;
    }
        
    public Integer getVfcuMediaFileId () {
        return this.vfcuMediaFileId;
    }
    
    public void setMediaFileName (String mediaFileName) {
        this.mediaFileName = mediaFileName;
    }
    
    public void setVendorChecksum (String vendorChecksum) {
        this.vendorChecksum = vendorChecksum;
    }
    
    public void setVfcuMediaFileId (Integer vfcuMediaFileId) {
        this.vfcuMediaFileId = vfcuMediaFileId;
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
             
         try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
    
            if (rs.next()) {
                childVfcuMediaId = (rs.getInt(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for child media ID in DB", e );
        }
         
        return childVfcuMediaId;
    }
    
    public boolean populateMediaFileName () {
    
        String sql = "SELECT  media_file_name " +
                     "FROM     vfcu_media_file " +
                     "WHERE    vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 setMediaFileName(rs.getString(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return false;
        }
         
        return true;
    }
    
    public boolean populateVendorChecksum () {
    
        String sql = "SELECT  vendor_checksum " +
                     "FROM    vfcu_media_file " +
                     "WHERE   vfcu_media_file_id = " + getVfcuMediaFileId() + " ";
            
        logger.log(Level.FINEST, "SQL: {0}", sql);       
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                 setMediaFileName(rs.getString(1));
            }
                
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable get vendor checksum value", e );
                return false;
        }
         
        return true;
    }
    
}
