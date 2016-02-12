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
    private Integer pathId;
    private Integer renditionId;
    
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
    
    public Integer getRenditionId () {
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
    
    public void setRenditionId (Integer renditionId) {
        this.renditionId = renditionId;
    }
    
    public boolean insertNewRecord() {
          
        ResultSet rs = null;
        
        
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
                        getRenditionId() + ", " + 
                        getPathId() + "," +
                        "'" + getFileName() + "', " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        Integer.parseInt (CDIS.getProperty("mediaFormatID")) + ", " +
                        getPixelH() + ", " +
                        getPixelW() + ")" ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
        try (Statement stmt = CDIS.getCisConn().createStatement() ) {
           
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.fileId = rs.getInt(1);
            } 
            else {
                 throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error, unable to insert into Media Files table", e );
                return false;
        }finally {
                try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;

    }
    
    public boolean updateFileNameAndPath () {
        
       int recordsUpdated = 0;
       String sql = "UPDATE mediaFiles " +
                    "SET pathid = " + getPathId() + ", " +
                    "FileName = '" + getFileName() + "' " +
                    "WHERE RenditionID = " + getRenditionId() ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
      try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql) ) {
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in TMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error, unable to update Path and filename in mediaFiles table", e);
                return false;
        }
        return true;
    }

}
