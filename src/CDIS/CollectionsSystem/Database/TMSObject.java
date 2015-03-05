/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.CollectionsSystem.Database;

import CDIS.CDIS;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;


        
public class TMSObject {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer objectID;
    String objectNumber;
    
    
    public int getObjectID () {
        return this.objectID;
    }
    
    public String getObjectNumber () {
        return this.objectNumber;
    }
    
   
    public void setObjectID (int objectID) {
        this.objectID = objectID;
    }
    
    private void setObjectNumber (String objectNumber) {
        this.objectNumber = objectNumber;
    }
    
    public boolean populateObjectFromRenditionNumber (String damsRenditionName, Connection tmsConn){

        String tmpObjectNumber = damsRenditionName.substring(0, damsRenditionName.lastIndexOf("_"));
        setObjectNumber(tmpObjectNumber.replaceAll("_", "."));
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql =    "select ObjectID " +
                        "from Objects " +
                        "where ObjectNumber = '" + getObjectNumber() + "'";
                    
                logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                setObjectID(rs.getInt(1));
            }
            else {
                return false;
            }
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public void populateObjectIDByRenditionId (Integer renditionId, Connection tmsConn) {
        
        PreparedStatement stmt = null;
        ResultSet rs = null;
        
        String sql =    "select a.ObjectID " +
                        "from Objects a, " +
                        "MediaXrefs b, " +
                        "MediaMaster c, " +
                        "MediaRenditions d " +
                        "where a.ObjectID = b.ID " +
                        "and b.MediaMasterID = c.MediaMasterID " +
                        "and b.TableID = 108 " +
                        "and c.MediaMasterID = d.MediaMasterID " +
                        "and d.RenditionID = " + renditionId;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try {
            stmt = tmsConn.prepareStatement(sql);                                
            rs = stmt.executeQuery();
            
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getInt(1) + " For RenditionID: " + renditionId);
                setObjectID(rs.getInt(1));
            }
        } catch (Exception e) {
                e.printStackTrace();
        
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
    }
    
}

