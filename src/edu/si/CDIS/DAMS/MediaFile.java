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


public class MediaFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    String mediaPathLocation;
    String mediaDrive;
    Connection cisConn;

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
                
		stmt = cisConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    String mediaPath = rs.getString(1);
 
                    this.mediaPathLocation = mediaPath;            
                }        
               
        }
            
	catch(Exception e) {
		e.printStackTrace();
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
    }
    
    public boolean create(CDIS cdis, String tmsFileName, int renditionID, Connection cisConn){
        
        this.cisConn = cisConn;
        
        logger.log(Level.FINEST, "mediaFile Name : " + tmsFileName);
        
        //Get the full tms pathname from the RenditionID
        populateMediaPathLocation (renditionID);
        
        logger.log(Level.FINEST, "mediaFile Path : " + mediaPathLocation);
        
        // configure from and to filenames
        File sourceFile = new File(mediaPathLocation + tmsFileName);
         
        File destFile = new File (cdis.properties.getProperty("workFolder") + "//" + sourceFile.getName());
                        
        logger.log(Level.FINEST, "Copying mediaFile from : " + mediaPathLocation + tmsFileName);
        logger.log(Level.FINEST, "Copying mediaFile to WorkFolder location: " + cdis.properties.getProperty("workFolder") + "//" + sourceFile.getName());
        
        try {
            // Copy from tms source location to workfile location
            FileUtils.copyFile(sourceFile, destFile);
           
            
        } catch (Exception e) {
            logger.log(Level.FINEST, "Error: Unable to move file to WorkFolder");
            e.printStackTrace();
            return false;
        }
        
        return true;
        
    }
}
