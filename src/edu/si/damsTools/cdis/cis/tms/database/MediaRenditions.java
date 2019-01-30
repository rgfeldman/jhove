/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.tms.modules.ModuleType;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.utilities.StringUtils;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
        
        ResultSet rs = null;       
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
                        Integer.parseInt (XmlUtils.getConfigValue("mediaStatusID")) + ", " +
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
    
    
    public void populateRemarksFromDams(DamsRecord damsRecord) {
        if (! (damsRecord.getSiAssetMetadata().getMaxIdsSize() == null )) {
            setRemarks("[MAX IDS SIZE = " + damsRecord.getSiAssetMetadata().getMaxIdsSize() + "]");
        }
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
    
    public boolean calculateRenditionNumber ( DamsRecord damsRecord, ModuleType module) {
       
        String extensionlessName = StringUtils.getExtensionlessFileName(damsRecord.getUois().getName());
  
        String charRank = null;
        
        if (extensionlessName.contains("_")) {               
            charRank = extensionlessName.substring(extensionlessName.lastIndexOf('_') +1 );
        }
        
        if (module.returnMappedMethod().equals("barcode")) {
            
            if (XmlUtils.getConfigValue("appendTimeToNumber").equals("true"))  {
                DateFormat df = new SimpleDateFormat("kkmmss");
                renditionNumber = module.returnRecordId() + "_" + String.format("%03d", charRank) + "_" + df.format(new Date()  );
            }
            else {
                if (charRank == null) {
                    renditionNumber = module.returnRecordId().toString();
                }    
                else {
                    renditionNumber = module.returnRecordId() + "_" + charRank;
                }    
            }
            
            return true;
        }
        
        logger.log(Level.FINER, "Dams Image fileName before formatting: {0}", damsRecord.getUois().getName());
                
        String tmsDelimiter = XmlUtils.getConfigValue("tmsDelimiter");
        String damsDelimiter = XmlUtils.getConfigValue("damsDelimiter");
        
        // If the delimeter is different from the image to the renditionNumber, we need to put the appropriate delimeter in the newly created name
        if (tmsDelimiter == null ||  tmsDelimiter.equals (damsDelimiter) ) {
            renditionNumber = StringUtils.getExtensionlessFileName(damsRecord.getUois().getName());
        }
        else {
            renditionNumber = StringUtils.getExtensionlessFileName(damsRecord.getUois().getName()).replaceAll(damsDelimiter, tmsDelimiter);
        }
        
        return true;
          
    }
}
