/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS;

import edu.si.CDIS.CDIS;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import edu.si.CDIS.DAMS.Database.CDISForIngest;
import edu.si.CDIS.utilties.ReformatPath;


public class MediaFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    String mediaPathLocation;
    String mediaDrive;
    Connection cisConn;
    Connection damsConn;
    String errorCode;

    public boolean populateMediaPathLocationCDIS (String cisUniqueMediaId, String siHoldingUnit) {
        
        String sql = "SELECT file_path " +
                    "FROM   cdis_for_ingest " +
                    "WHERE  cis_unique_media_id = '" + cisUniqueMediaId + "' " +
                    "AND    si_holding_unit = '" + siHoldingUnit + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    this.mediaPathLocation = rs.getString(1);           
                }     
                else {
                    throw new Exception();
                }
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Find FilePath in CDIS, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
    }
    
    
    public boolean populateMediaPathLocationTMS (String cisUniqueMediaId) {
        
        String sql = "Select Path " +
                    "From MediaPaths  mp, " +
                    "MediaFiles mf " +
                    "Where mp.PathID = mf.PathID " +
                    "AND RenditionID = " + cisUniqueMediaId;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
                
		stmt = cisConn.prepareStatement(sql);
		rs = stmt.executeQuery();
                    
                if (rs.next()) {
                    this.mediaPathLocation = rs.getString(1);           
                }        
                else {
                    throw new Exception();
                }
        }
            
	catch(Exception e) {
		logger.log(Level.SEVERE, "Error: Unable to Find FilePath in TMS, returning", e);
                return false;
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        return true;
        
    }
    
    public boolean sendToIngest(CDIS cdis, String cisFileName, String cisUniqueMediaId){
        
        this.damsConn = cdis.damsConn;
        boolean pathFound = false;
        String baseDir = (cdis.properties.getProperty("hotFolderBaseDir"));
        
        
        logger.log(Level.FINEST, "mediaFile Name : " + cisFileName);
        
        try {
            //Get the full tms pathname from the RenditionID
            switch (cdis.properties.getProperty("cisSourceDB")) {
                case "TMSDB" :
                    this.cisConn = cdis.cisConn;
                    pathFound = populateMediaPathLocationTMS (cisUniqueMediaId);
                break;
                
                case "CDISDB" :
                    pathFound = populateMediaPathLocationCDIS (cisUniqueMediaId, cdis.properties.getProperty("siHoldingUnit"));
                break;
            
                default:     
                    logger.log(Level.SEVERE, "Error: Invalid ingest source {0}, returning", cdis.properties.getProperty("cisSourceDB") );
                    return false;
            }
        
            if (! pathFound) {
                logger.log(Level.FINEST, "returning...path not found");
                this.errorCode = "FPE";
                return false;
            }
         
            logger.log(Level.FINEST, "mediaFile Path : " + mediaPathLocation);

            // Build the string to contain the 'from' path and filename
            String cisPathwithFile = cdis.properties.getProperty("cisMediaDrive") + "\\" + mediaPathLocation + "\\" + cisFileName;
            ReformatPath reformatPath = new ReformatPath();
            cisPathwithFile = reformatPath.reformatPathMS(cisPathwithFile);
                                
            // Build the string to contain the 'to' path and filename
            //get the hotfolder Location for this record
            CDISForIngest forIngest = new CDISForIngest();
            forIngest.setCisUniqueMediaId(cisUniqueMediaId);
            forIngest.setSiHoldingUnit(cdis.properties.getProperty("siHoldingUnit"));           
            forIngest.populateHotFolder(damsConn);
            
            String workFileBatchLocation = baseDir + "\\" + forIngest.getHotFolder() + "\\TEMP-XFER\\" + cdis.getBatchNumber();
            //reformat the string to correct
            
            workFileBatchLocation = reformatPath.reformatPathMS(workFileBatchLocation);
            
            // set 'to' and 'from' files
            File sourceFile = new File(cisPathwithFile);            
            File destDir = new File (workFileBatchLocation);
            
            logger.log(Level.FINEST, "Copying mediaFile from Original location : " + mediaPathLocation + "\\" + cisFileName);
            logger.log(Level.FINEST, "Copying mediaFile to WorkFolder location: " + workFileBatchLocation + "\\" + sourceFile.getName());
        
            // Copy from tms source location to workfile location (and put in subdirectory with batch name).
            FileUtils.copyFileToDirectory(sourceFile, destDir, true);
           
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error: Unable to physically copy file to Work Folder. returning", e);
            this.errorCode = "FCW";
            return false;
        }
        
        return true;
        
    }
}
