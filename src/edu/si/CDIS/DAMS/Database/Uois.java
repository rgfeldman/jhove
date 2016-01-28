/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class Uois {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private String uoiid;
    private String name;
    private Integer bitmapHeight;
    private Integer bitmapWidth; 
    
    public String getName () {
        return this.name;
    }
    
    public void setName (String name) {
        this.name = name;
    }
    
    public String getUoiid () {
        return this.uoiid;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
    
    public boolean populateName () {
  
        String sql = "SELECT name FROM towner.uois " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            if (rs != null && rs.next()) {
                setName (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain name from uois table", e );
                return false;
        }
        
        return true;
    }
    
    public boolean populateUoisData() {
        
        String sql = "SELECT name, bitmap_height, bitmap_width " +
                    "FROM towner.uois " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql); 
              ResultSet rs = pStmt.executeQuery() ) {
              
            if (rs.next()) {
                setName(rs.getString("name"));
                this.bitmapHeight = rs.getInt("bitmap_height");
                this.bitmapWidth = rs.getInt("bitmap_width");
            }        
        } catch (Exception e) {
                logger.log(Level.FINER, "unable to get uois information from database", e );
                return false;
        }
       
        return true;
    }   
    
    /*  Method :        updateMetaDataStateDate
        Arguments:      
        Returns:      
        Description:    Update the Uois table with the MetaDataStateDate.  This will trigger IDS.
        RFeldman 2/2015
    */
    public Integer updateMetaDataStateDate() {
        
        int recordsUpdated = 0;
        
        // We have not met any of the above conditions, we should update for IDS
        String sql = "UPDATE towner.uois " +
                    "SET metadata_state_dt = SYSDATE, " +
                    "    metadata_state_user_id = '22246' " +
                    "WHERE uoi_id = '" + this.uoiid + "'";
        
        logger.log(Level.ALL, "updateUOIIS Statment: " + sql);
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql) ) {
            
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to update UOIS table with new date", e );
        }
            
        return recordsUpdated;

    }
    
    public boolean populateUoiidForNameChksum(String checksum) {
  
        String sql = "SELECT    a.uoi_id " +
                    "FROM       towner.uois a, " +
                    "           towner.checksum_view b " +
                    "WHERE      a.uoi_id = b.uoi_id " +
                    "AND        a.content_state = 'NORMAL' " +
                    "AND        a.content_type != 'SHORTCUT' " +
                    "AND        a.name = '" + getName() + "' " +
                    "AND        b.content_checksum = '" + checksum + "'";
        
        try (PreparedStatement pStmt = CDIS.getDamsConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            logger.log(Level.FINEST,"SQL! " + sql); 
            
            if (rs != null && rs.next()) {
                setUoiid (rs.getString(1));
                return true;
            }   
            else {
                return false;
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain name from uois table", e );
                return false;
        
        }
    }
}
