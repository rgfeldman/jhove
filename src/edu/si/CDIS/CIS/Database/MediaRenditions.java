/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.DataProvider;
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
public class MediaRenditions {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    int renditionId;
    String renditionNumber;
            
    public int getRenditionId () {
        return this.renditionId;
    }
    
    public String getRenditionNumber() {
        return this.renditionNumber;
    }
    
    
    private void setRenditionId (int renditionId) {
        this.renditionId = renditionId; 
    }
    
    public void setRenditionNumber (String renditionNumber) {
        this.renditionNumber = renditionNumber;
    }
    
    
    
    public void populateIdByRenditionNumber (Connection cisConn) {
        String sql = "Select max (RenditionID) " +
                     "From MediaRenditions " +
                     "Where RenditionNumber = '" + getRenditionNumber() + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        try {
		stmt = cisConn.prepareStatement(sql);
		rs = stmt.executeQuery();
              
                if (rs.next()) {
                    setRenditionId(rs.getInt(1));
                }        
	}
            
	catch(SQLException sqlex) {
		sqlex.printStackTrace();
	}
        finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
	}
        
        
    }
    public boolean insertNewRecord(CDIS cdis, Integer mediaMasterID ) {
        
        Integer mediaTypeID = null;
        Integer mediaStatusID = null;
        Statement stmt = null;
        
        // Get variables from the properties list
        try {
            mediaTypeID = Integer.parseInt (cdis.properties.getProperty("mediaTypeID"));
            mediaStatusID = Integer.parseInt (cdis.properties.getProperty("mediaStatusID"));
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
                        mediaTypeID + ", " +
                        "1, " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        "'[MAX IDS SIZE = 0]', " +
                        mediaStatusID + ", " +
                        "CONVERT (date,SYSDATETIME()))";
        
        logger.log(Level.FINER, "SQL: {0}", sql);
        
        try {
            stmt = cdis.cisConn.createStatement();
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            ResultSet rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.renditionId = rs.getInt(1);
            }    
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
                              
    }
    
    
    public int setFileId(Connection cisConn, Integer fileId) {
        int updateCount;
        
        String sql = "update MediaRenditions " +
                    "set PrimaryFileID = " + fileId + " " +
                    "where renditionID = " + getRenditionId() ;
                               
        updateCount = DataProvider.executeUpdate(cisConn, sql);
        
        return updateCount;
    }
   
}
