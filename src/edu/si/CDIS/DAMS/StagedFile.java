/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CdisCisMediaTypeR;

/**
 *
 * @author rfeldman
 */
public class StagedFile {
   
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String fileName;
    private String basePath;
    private String pathEnding; 
    
    public String getFileName () {
        return this.fileName;
    }
    
    public String getBasePath () {
        return this.basePath;
    }
    
    public String getPathEnding () {
        return this.pathEnding;
    }
    
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    public void setBasePath (String basePath) {
        this.basePath = basePath;
    }
    
    public void setPathEnding (String pathEnding) {
        this.pathEnding = pathEnding;
    }
    
    public boolean populateNamePathFromId (Integer vfcuMediaFileId) {
 
        String sql = "SELECT b.media_file_name, a.base_path_staging, a.file_path_ending " +
                     "FROM  vfcu_md5_file a, " +
                     "      vfcu_media_file b " +
                     "WHERE a.vfcu_md5_file_id = b.vfcu_md5_file_id " +
                     "AND   b.vfcu_media_file_id = " + vfcuMediaFileId;
                   
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql); 
             ResultSet  rs = pStmt.executeQuery() ) {    

            if (rs.next()) {
                setFileName(rs.getString(1));
                setBasePath(rs.getString(2));
                setPathEnding(rs.getString(3));
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
                return false; 
        }
        return true;
    }
    
    // Moves the staged file to the MASTER folder
    public boolean deliverSubFile (String destination) {

        String fileNamewithPath = getBasePath() + "\\" + getPathEnding() + "\\" + getFileName();
        String postIngestDeliveryLoc = destination + "\\" + getPathEnding();
        
        logger.log(Level.FINER,"File moved from staging location: " + fileNamewithPath );
        logger.log(Level.FINER,"File moved to emuPickup location: " + postIngestDeliveryLoc );
            
        try {            
            Path sourceFile      = Paths.get(fileNamewithPath);
            Path destPath = Paths.get(postIngestDeliveryLoc);
            Path destWithFile = Paths.get(postIngestDeliveryLoc + "\\" + getFileName());

            // create the directory if we need it
            Files.createDirectories(destPath);
            
            //move the files
            Files.move(sourceFile, destWithFile);
            
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when moving to delivery location",e);
            return false;
        }
        
        return true;
    }
    
    
    // Moves the staged file to the MASTER folder
    public boolean xferToHotFolder (String destination, Integer cdisMapId) {
        
        String fileNamewithPath = getBasePath() + "\\" + getPathEnding() + "\\" + getFileName();
        String hotFolderDestStr = destination + "\\" + getFileName();
        
        logger.log(Level.FINER,"File xferred from staging location: " + fileNamewithPath );
        logger.log(Level.FINER,"File xferred to hotfolder location: " + hotFolderDestStr );
            
        //Determine file transfer type
        CDISMap cdisMap = new CDISMap();
        cdisMap.setCdisMapId(cdisMapId);
        cdisMap.populateCdisCisMediaTypeId();
        
        CdisCisMediaTypeR cdisCisMediaTypeR = new CdisCisMediaTypeR();
        cdisCisMediaTypeR.setCdisCisMediaTypeId(cdisMap.getCdisCisMediaTypeId());
        
        cdisCisMediaTypeR.populatePostIngestDelivery();
        
        try {
            Path source      = Paths.get(fileNamewithPath);
            Path destWithFile = Paths.get(hotFolderDestStr);
            
            if (cdisCisMediaTypeR.getPostIngestDelivery().equals("Y")) {
                Files.copy(source, destWithFile);
            }
            else {
                Files.move(source, destWithFile);
            }
        } catch (Exception e) {
            logger.log(Level.FINER,"ERROR encountered when xFerrring to hot folder",e);
            return false;
        }
        
        return true;
    }
}
    