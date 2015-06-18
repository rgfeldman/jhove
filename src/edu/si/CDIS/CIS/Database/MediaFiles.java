/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaFiles {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
 
    int fileId;
    boolean isPrimary;
    int rank;
    
    int pixelH;
    int pixelW;
    
    public int getFileId () {
        return this.fileId;
    }
    
    public int getPixelH () {
        return this.pixelH;
    }
    
    public int getPixelW () {
        return this.pixelW;
    }
    
    private void setPixelH (int pixelH) {
        this.pixelH = pixelH;
    }
    
    private void setPixelW (int pixelW) {
        this.pixelW = pixelW;
    }
    
     /*  Method :        populateRenditionFromDamsInfo
        Arguments:      
        Description:    determines and populates the mediaFiles member variables from information in DAMS
        RFeldman 3/2015
    */
    public String populateFromDams(String uoiid, Connection damsConn) {
        
        String sql = "SELECT name, bitmap_height, bitmap_width from UOIS where UOI_ID = '" + uoiid + "'";
        String imageName = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = damsConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    imageName = rs.getString(1);
                   
                    this.setPixelH(rs.getInt("bitmap_height"));
                    this.setPixelW(rs.getInt("bitmap_width"));
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
                return "";
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
       
        return imageName;
    }   
  
    
    public boolean insertNewRecord(CDIS cdis, String uan, Integer renditionId, String fileType, Integer height, Integer width) {
        
        Integer mediaFormatId = null;
        Integer pathId = null;
        Statement stmt = null;
        
         // Get variables from the properties list
        try {
            mediaFormatId = Integer.parseInt (cdis.properties.getProperty("mediaFormatID"));
            
            if (fileType.equalsIgnoreCase("PDF")) {
                pathId = Integer.parseInt (cdis.properties.getProperty("PDFPathId"));
            }
            else {
                pathId = Integer.parseInt (cdis.properties.getProperty("IDSPathId"));
            }
             
        } catch (Exception e) {
                e.printStackTrace();
                logger.log(Level.FINER, "Error: unexpected property values required for insert into MediaFiles table");
                return false;
        }
        
       
        
        // From Anacostia
       String sql = "insert into MediaFiles " +
                        "(RenditionID, " +
                        "PathID, " +
                        "FileName, " +
                        "LoginID, " +
                        "EnteredDate, " +
                        "FormatID, " +
                        "PixelH, " +
                        "PixelW) " +
                    " values ( " +
                        renditionId + ", " + 
                        pathId + "," +
                        "'" + uan + "', " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        mediaFormatId + ", " +
                        height + ", " +
                        width + ")";
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
        try {
            stmt = cdis.cisConn.createStatement();
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            ResultSet rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.fileId = rs.getInt(1);
            }    
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;

    }
    
}
