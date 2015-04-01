/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.DAMS;

import CDIS.CDIS;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;


public class MediaFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    String mediaPathLocation;
    Connection tmsConn;

    public void populateMediaPathLocation(int renditionID) {
        
        
        String sql = "Select Path " +
                    "From MediaPaths  mp, " +
                    "MediaFiles mf " +
                    "Where mp.PathID = mf.PathID " +
                    "AND RenditionID = " + renditionID;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = tmsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    this.mediaPathLocation = rs.getString(1);
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
    }
    
    public void create(CDIS cdis_new, String tmsFileName, int renditionID, Connection tmsConn){
        
        this.tmsConn = tmsConn;
        String pathlessFileName = tmsFileName;
        String sourceFilePath = null;
        String destinationFilePath = null;
        
        logger.log(Level.FINEST, "mediaFile Name : " + tmsFileName);
        logger.log(Level.FINEST, "mediaFile Path : " + mediaPathLocation);
               
        //Get the full tms pathname from the RenditionID
        populateMediaPathLocation (renditionID);
        
        // configure from and to filenames
        sourceFilePath = mediaPathLocation + tmsFileName;  
        File sourceFile = new File(sourceFilePath);
        
        if (tmsFileName.contains("\\")) {
            pathlessFileName = tmsFileName.substring(tmsFileName.lastIndexOf("\\"));
        }
        
        if (tmsFileName.contains("/")) {
            pathlessFileName = tmsFileName.substring(tmsFileName.lastIndexOf("/"));
        }
        
        destinationFilePath = cdis_new.properties.getProperty("workFolder") + "\\" + pathlessFileName; 
        File destFile = new File (destinationFilePath);
                        
        logger.log(Level.FINEST, "Copying mediaFile from : " + sourceFilePath);
        logger.log(Level.FINEST, "Copying mediaFile to: " + destinationFilePath);
        
        try {
            // Copy from tms source location to workfile location
            FileUtils.copyFile(sourceFile, destFile);
            
        } catch (Exception e) {
                    e.printStackTrace();
        }
        
    }
}
