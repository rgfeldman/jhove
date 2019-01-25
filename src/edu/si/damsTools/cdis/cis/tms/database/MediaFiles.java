/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MediaFiles {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
 
    private int fileId;
    private String fileName;
    private Integer pathId;
    private Integer mediaFormatId;
    private int pixelH;
    private int pixelW;
    private Integer renditionId;
       
    public int getFileId () {
        return this.fileId;
    }
    
    public String getFileName () {
        return this.fileName;
    }
    
    public Integer getMediaFormatId () {
        return this.mediaFormatId;
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
    
    public void setMediaFormatId (int mediaFormatId) {
        this.mediaFormatId = mediaFormatId;
    }
    
    public void setPathId (Integer pathId) {
        this.pathId = pathId;
    }
      
    public void setPixelH (int pixelH) {
        this.pixelH = pixelH;
    }
    
    public void setPixelW (int pixelW) {
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
                        getMediaFormatId() + ", " +
                        getPixelH() + ", " +
                        getPixelW() + ")" ;
       
       logger.log(Level.FINER, "SQL: {0}", sql);
        
        try (Statement stmt = DamsTools.getCisConn().createStatement() ) {
           
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
    
    public int returnIDForFileName () {
        
        int id = 0;
        
        String sql = "SELECT FileId " +
                     "FROM mediaFiles " + 
                     "WHERE FileName = '" + getFileName() + "'";
                     
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
    
    public boolean setValuesFromDams (DamsRecord damsRecord) {
        
        switch  (damsRecord.getUois().getMasterObjMimeType()) {
            case "image/jpeg" :
                setMediaFormatId(Integer.parseInt(XmlUtils.getConfigValue("jpgFormatId")));
                break;
            case "image/tiff" :
                setMediaFormatId(Integer.parseInt(XmlUtils.getConfigValue("tifFormatId")));
                break;
            case "application/pdf":
                setMediaFormatId(Integer.parseInt(XmlUtils.getConfigValue("pdfFormatId")));
                break;
            case "audio/x-mpeg" :
            case "audio/x-wav" :
                setMediaFormatId(Integer.parseInt(XmlUtils.getConfigValue("audioFormatId")));
                break;
            case "video/mpeg" :
            case "video/mxf" :
                setMediaFormatId(Integer.parseInt(XmlUtils.getConfigValue("videoFormatId")));
                break;
            default :
                logger.log(Level.FINER, "unable to get valid mimeType from DAMS: " + damsRecord.getUois().getMasterObjMimeType() );
                return false;
        }
        
        if (damsRecord.getUois().getMasterObjMimeType().equals("application/pdf")) {
            setPathId(Integer.parseInt (XmlUtils.getConfigValue("pdfPathId")));
            setFileName (damsRecord.getSiAssetMetadata().getOwningUnitUniqueName() +  ".pdf");
        }
        else {
            setPathId (Integer.parseInt (XmlUtils.getConfigValue("idsPathId")));
            setFileName(damsRecord.getSiAssetMetadata().getOwningUnitUniqueName());
        } 
        
        setPixelH(damsRecord.getUois().getBitmapHeight());
        setPixelW(damsRecord.getUois().getBitmapWidth());
        
        return true;
    }

}
