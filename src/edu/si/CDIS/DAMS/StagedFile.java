/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISActivityLog;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.utilties.ErrorLog;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import java.sql.Connection;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author rfeldman
 */
public class StagedFile {
    

    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String fileName;
    private String path;
    
    public String getFileName () {
        return this.fileName;
    }
    
    public String getPath () {
        return this.path;
    }
    
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    public void setPath (String path) {
        this.path = path;
    }
    
    public boolean populateNamePathFromId (Connection dbConn, Integer vfcuMediaFileId) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT b.media_file_name, a.staging_file_path " +
                         "FROM  vfcu_md5_file a, " +
                         "      vfcu_media_file b " +
                         "WHERE a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                         "AND   b.vfcu_media_file_id = " + vfcuMediaFileId;
                   
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = dbConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                setFileName(rs.getString(1));
                setPath(rs.getString(2));
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public String retrieveSubFileId (Connection dbConn, String vfcuMasterMediaId) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        String childMediaId = null;
        
        try {
            String sql = "SELECT  aa.vfcu_media_file_id, " + 
                         "aa.media_file_name " +
                         "FROM  vfcu_media_file a, " +       
                         "  vfcu_md5_file b, " +
                         "  vfcu_media_file aa, " +
                         "  vfcu_md5_file bb " +
                         "WHERE a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                         "AND   aa.vfcu_md5_file_id = bb.vfcu_md5_file_id " +
                         "AND   bb.master_md5_file_id != bb.VFCU_MD5_FILE_ID " +
                         "AND   SUBSTR(a.media_file_name, 0, INSTR(a.media_file_name, '.')-1) = " + " SUBSTR(aa.media_file_name, 0, INSTR(aa.media_file_name, '.')-1) " +
                         "AND   a.vfcu_media_file_id = " + vfcuMasterMediaId; 
                   
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = dbConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                childMediaId = (rs.getString(1));
                setFileName(rs.getString(2));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for child media ID in DB", e );
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return childMediaId;
    }
    
    // Moves the staged file to the MASTER folder
    public boolean moveToMaster (String destination) {
        String fileNamewithPath = getPath() + "\\" + getFileName();
        File stagedFile = new File (fileNamewithPath);
        
        String hotFolderDestStr = destination + "\\" + "MASTER";
        File hotFolderDest = new File (hotFolderDestStr);
        
        try {
            
            FileUtils.moveFileToDirectory(stagedFile, hotFolderDest, false);
               
            logger.log(Level.FINER,"File moved from staging location: " + fileNamewithPath );
            logger.log(Level.FINER,"File moved to hotfolder location: " + hotFolderDestStr );
                    
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when moving to master directory",e);
            return false;
        }
        
    
        return true;
    }
    
    // Copies the staged file to the SUBFILE folder
    public boolean copyToSubfile (String destination) {
        String fileNamewithPath = getPath() + "\\" + getFileName();
        File stagedFile = new File (fileNamewithPath);
        
        String hotFolderDestStr = destination + "\\" + "SUBFILES";
        File hotFolderDest = new File (hotFolderDestStr);
       
        try {
            
            FileUtils.copyFileToDirectory(stagedFile, hotFolderDest, false);
            
            logger.log(Level.FINER,"File copied from staging location: " + fileNamewithPath );
            logger.log(Level.FINER,"File copied to hotfolder location: " + hotFolderDestStr );
               
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when copying to subFolder directory",e);
            return false;
        }
                                 
        return true;
    }
    

    private void createReadyFile (String hotDirectoryName) {
    
        String readyFilewithPath = null;
        
        try {
                //Create the ready.txt file and put in the media location
                readyFilewithPath = hotDirectoryName + "\\ready.txt";

                logger.log(Level.FINER, "Creating ReadyFile: " + readyFilewithPath);
                
                File readyFile = new File (readyFilewithPath);
            
                readyFile.createNewFile();
  
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when trying to create ready.txt file",e);;
        }
    }
    
}
    