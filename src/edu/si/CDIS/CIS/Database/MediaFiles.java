/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MediaFiles {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
 
    private int fileId;
    private String fileName;
    private boolean isPrimary;
    private int rank;
    private Integer pathId;
    private String renditionId;
    
    private int pixelH;
    private int pixelW;
    
    public int getFileId () {
        return this.fileId;
    }
    
    public String getFileName () {
        return this.fileName;
    }
    
    public int getPathId () {
        return this.pathId;
    }
    
    public int getPixelH () {
        return this.pixelH;
    }
    
    public int getPixelW () {
        return this.pixelW;
    }
    
    public String getRenditionId () {
        return this.renditionId;
    }
    
    public void setFileName (String fileName) {
        this.fileName = fileName;
    }
    
    public void setPathId (Integer pathId) {
        this.pathId = pathId;
    }
      
    private void setPixelH (int pixelH) {
        this.pixelH = pixelH;
    }
    
    private void setPixelW (int pixelW) {
        this.pixelW = pixelW;
    }
    
    public void setRenditionId (String renditionId) {
        this.renditionId = renditionId;
    }
        
    
     /*  Method :        populateRenditionFromDamsInfo
        Arguments:      
        Description:    determines and populates the mediaFiles member variables from information in DAMS
        RFeldman 3/2015
    */
    public String populateFromDams(String uoiid) {
        
        String sql = "SELECT name, bitmap_height, bitmap_width from UOIS where UOI_ID = '" + uoiid + "'";
        String imageName = null;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = CDIS.getDamsConn().prepareStatement(sql);
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
  
    
    public boolean insertNewRecord(String uan, Integer renditionId, String fileType) {
        
        Integer mediaFormatId = null;
        Integer pathId = null;
        String fileName = null;
        
        Statement stmt = null;
        ResultSet rs = null;
        
        
         // Get variables from the properties list
        try {
            mediaFormatId = Integer.parseInt (CDIS.getProperty("mediaFormatID"));
            
            if (fileType.equalsIgnoreCase("PDF")) {
                pathId = Integer.parseInt (CDIS.getProperty("PDFPathId"));
                fileName = uan + ".pdf";
            }
            else {
                pathId = Integer.parseInt (CDIS.getProperty("IDSPathId"));
                fileName = uan;
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
                        "'" + fileName + "', " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        mediaFormatId + ", " +
                        getPixelH() + ", " +
                        getPixelW() + ")";
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
        try {
            stmt = CDIS.getCisConn().createStatement();
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.fileId = rs.getInt(1);
            }    
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;

    }
    
    public boolean updateFileNameAndPath () {
        
       int recordsUpdated = 0;
       PreparedStatement pStmt = null;
        
       String sql = "UPDATE mediaFiles " +
                    "SET pathid = " + getPathId() + " " +
                    "FileName = " + getFileName() + " " +
                    "WHERE RenditionID = " + getRenditionId() ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
      try {
            
            pStmt = CDIS.getCisConn().prepareStatement(sql);
            recordsUpdated = pStmt.executeUpdate(sql);
            
            logger.log(Level.FINEST,"Rows Updated in TMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
      
        return true;
    }

}
