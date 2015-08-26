/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

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
public class SiAdminContentTypeDtls {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());

    String adminContentType;
    String uoiid;
    
    public String getAdminContentType () {
        return adminContentType;
    } 
    
    public String getUoiid () {
        return uoiid;
    } 
    
    public void setAdminContentType(String adminContentType) {
	this.adminContentType = adminContentType;
    }
    
    public void setUoiid(String uoiid) {
	this.uoiid = uoiid;
    }
    
    public boolean populateAdminType (Connection damsConn, Integer renditionId) {
    
        ResultSet rs = null;
        PreparedStatement stmt = null;
        
        String sql =    "SELECT mx.PrimaryDisplay " +
                        "FROM   mediaXrefs mx, " +
			"       mediaRenditions mr " +
                        "WHERE	mx.MediaMasterID = mr.MediaMasterID " +
                        "AND	mx.TableID = 108 " +	
                        "AND    isPrimary = 1 " +
                        "AND	mr.RenditionID = " + renditionId;
        try {
            stmt = damsConn.prepareStatement(sql);
            rs = stmt.executeQuery();
              
            if (rs.next()) {
                setAdminContentType("SD600_P");
            }        
            else {
                setAdminContentType("SD600");
            }
           
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
        
    }
     
    public int updateAdminContentType (Connection damsConn) {
        int recordsUpdated = 0;
        Statement stmt = null;
        
        String sql = "update si_admin_content_type_dtls set adminContentType = '" + this.adminContentType + "' " +
                    "where UOI_ID = '" + uoiid + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try {
            recordsUpdated = DataProvider.executeUpdate(damsConn, sql);
        
            logger.log(Level.FINEST,"si_admin_content_type Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
            
        return recordsUpdated;
        
    }
    
}
