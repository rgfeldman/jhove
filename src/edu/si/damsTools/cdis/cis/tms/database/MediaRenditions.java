/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;
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
public class MediaRenditions {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Integer fileId;
    private Integer mediaTypeId;
    private Integer renditionId;
    private String renditionNumber;
    private String remarks;
        
    public Integer getFileId () {
        return this.fileId;
    }
    
    public Integer getMediaTypeId () {
        return this.mediaTypeId;
    }
    
    public String getRemarks () {
        return this.remarks;
    }
        
    public Integer getRenditionId () {
        return this.renditionId;
    }
    
    public String getRenditionNumber() {
        return this.renditionNumber;
    }
    
    
    public void setFileId (Integer fileId) {
        this.fileId = fileId; 
    }
    
    public void setMediaTypeId (Integer mediaTypeId) {
        this.mediaTypeId = mediaTypeId; 
    }
        
    public void setRemarks (String remarks) {
        this.remarks = remarks;
    }
    
    public void setRenditionId (Integer renditionId) {
        this.renditionId = renditionId; 
    }
    
    public void setRenditionNumber (String renditionNumber) {
        this.renditionNumber = renditionNumber;
    }
    
    
    
    public boolean insertNewRecord(Integer mediaMasterID ) {
        
        Integer mediaStatusID = null;
        ResultSet rs = null;
        
        // Get variables from the properties list
        try {
            mediaStatusID = Integer.parseInt (XmlUtils.getConfigValue("mediaStatusID"));
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }
        
        String sql = "insert into MediaRenditions " +
                        "(MediaMasterID, " +
                        "RenditionNumber, " +
                        "PrimaryFileID, " +
                        "ParentRendID, " +
                        "MediaTypeID, " +
                        "IsColor, " +
                        "LoginID, " +
                        "EnteredDate, " +
                        "Remarks, " +
                        "MediaStatusID, " +
                        "RenditionDate) " +
                    "values (" + mediaMasterID + ", " + 
                        "'" + getRenditionNumber() + "', " +
                        "-1, " +
                        "-1, " +
                        getMediaTypeId() + ", " +
                        "1, " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        "'" + getRemarks() + "', " +
                        mediaStatusID + ", " +
                        "CONVERT (date,SYSDATETIME()))";
        
        logger.log(Level.FINER, "SQL: {0}", sql);
        
        try (Statement stmt = DamsTools.getCisConn().createStatement() ) {

            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.renditionId = rs.getInt(1);
            }    
            else {
                 throw new Exception();
            }
            
        } catch (Exception e) {
           logger.log(Level.FINER, "Error, unable to insert into MediaRenditions table", e );
            return false;
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;         
    }
    
    public int returnIDForRenditionNumber () {
        
        int id = 0;
        
        String sql = "SELECT RenditionId " +
                     "FROM mediaRenditions " + 
                     "WHERE RenditionNumber = '" + getRenditionNumber() + "'";
                     
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery();   ){
            
            if (rs.next()) {
                id = rs.getInt(1);
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to check for existing Md5File in DB", e );
        }
        return id;
       
    }
    
    public boolean updateFileId() {
        int updateCount;
        
        String sql = "update MediaRenditions " +
                    "set PrimaryFileID = " + getFileId() + " " +
                    "where renditionID = " + getRenditionId() ;
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql)) {
            updateCount = pStmt.executeUpdate();
            
            if (updateCount != 1) {
                throw new Exception();
            }
        
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to update FileId in mediaRenditions table", e );
                return false;
        }  
        
        return true;
        
    }
    
}
