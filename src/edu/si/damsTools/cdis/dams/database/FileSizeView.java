/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams.database;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class FileSizeView {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String contentSize;
    private String uoiId;
    
    public String getContentSize() {
        return this.contentSize;
    }
    
    public String getUoiId() {
        return this.uoiId;
    }
    
    public void setContentSize(String contentSize) {
        this.contentSize = contentSize;
    }
    
    public void setUoiId(String uoiId) {
        this.uoiId = uoiId;
    }
    
    public boolean populateFileSizeInfo() {
        String sql = "SELECT replace(content_size,' ','') " +
                    "FROM towner.file_size_view " +
                    "WHERE uoi_id = '" + getUoiId() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs != null && rs.next()) {
                setContentSize (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain fileSize data from file_size_view", e );
                return false;
        }
        return true;
    }
       
}
