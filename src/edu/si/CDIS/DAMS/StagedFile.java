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
    
    public boolean populatePathFromVfcuId (Connection dbConn, Integer vfcuMediaFileId) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        try {
            String sql = "SELECT a.staging_file_path " +
                         "FROM  vfcu_md5_file a, " +
                         "      vfcu_media_file b " +
                         "WHERE a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                         "AND   b.vfcu_media_file_id = " + vfcuMediaFileId;
                   
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = dbConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs.next()) {
                setPath(rs.getString(1));
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
    
    public String retrieveSubFileInfo (Connection dbConn, String vfcuMasterMediaId) {
        PreparedStatement pStmt = null;
        ResultSet rs = null;
        
        String childMediaId = null;
        
        try {
            String sql = "SELECT a.vfcu_media_file_id, a.media_file_name " +
                        "FROM  vfcu_media_file a, " +
                        "      vfcu_md5_file b " +
                        "WHERE a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                        "AND   b.vfcu_master_md5_file_id != b.vfcu_md5_file_id " +
                        "AND   b.vfcu_master_md5_file_id = " + vfcuMasterMediaId;
                   
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
    
    
    public String sendToHotFolder (CDIS cdis) {
          
        String operationType = null;

        
        String fileNamewithPath = getPath() + "\\" + getFileName();
        String fileExtension = FilenameUtils.getExtension(getFileName());
        
        File stagedFile = new File (fileNamewithPath);
        
        String hotFolderBaseName = null;
        File hotFolderBase;
        
        boolean lastHotFolder = false;
        int hotFolderIncrement = 1;
        
        while (!lastHotFolder) {
           
            hotFolderBaseName = cdis.properties.getProperty("hotFolderArea") + "_" + hotFolderIncrement;
            hotFolderBase = new File (hotFolderBaseName);
            
            if (hotFolderBase.exists()) {
                //check if master area in hotfolder is empty
               
            }
            
            hotFolderIncrement ++;
            
        }
        

        
        if (fileExtension.equals(cdis.properties.getProperty("masterMediaType"))) {
            //move to hot folder master
            operationType = "FMM";
            try {
                String masterHotFolder = hotFolderBaseName + "\\" + "SUBFILES";
                File hotFolderDest = new File (masterHotFolder);
            
                FileUtils.moveFileToDirectory(stagedFile, hotFolderDest, false);
               
            } catch (Exception e) {
                logger.log(Level.FINER,"ERROR encountered when moving to subFolder directory",e);
            }
             
        }
        else if (fileExtension.equals(cdis.properties.getProperty("subFileMediaType"))) {
            //copy to hot folder subdir
            operationType = "FCS";
            
            try {
                String masterHotFolder = hotFolderBaseName + "\\" + "MASTER";
                File hotFolderDest = new File (masterHotFolder);
            
                FileUtils.copyFileToDirectory(stagedFile, hotFolderDest, false);
                
        
            } catch (Exception e) {
                logger.log(Level.FINER,"ERROR encountered when moving to subFolder directory",e);
            }
            
        }   
        else {
            //error
        }
                             
        return operationType;
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
    