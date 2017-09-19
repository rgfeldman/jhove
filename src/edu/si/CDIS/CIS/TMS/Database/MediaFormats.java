/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.TMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaFormats {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer mediaFormatId;
    Integer mediaTypeId;
    
    public Integer getMediaFormatId () {
        return this.mediaFormatId;
    }
    
    public Integer getMediaTypeId () {
        return this.mediaTypeId;
    }
    
    public void setMediaFormatId (Integer mediaFormatId) {
        this.mediaFormatId = mediaFormatId; 
    }
    
    public void setMediaTypeId (Integer mediaTypeId) {
        this.mediaTypeId = mediaTypeId; 
    }
    
    
    public boolean populateMediaType() {
         String sql = "SELECT mediaTypeId " +
                      "FROM mediaFormats " +
                      "WHERE FormatId = " + getMediaFormatId();
         
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                setMediaTypeId (rs.getInt(1));
            }   
            else {
                // we need a map id, if we cant find one then raise error
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain MediaType for formatID: " + getMediaFormatId() , e );
                return false;
        }
        return true;
    }
}
