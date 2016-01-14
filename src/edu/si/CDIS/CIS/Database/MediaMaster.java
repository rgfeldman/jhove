/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author rfeldman
 */
public class MediaMaster {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private Integer displayRendId;
    private Integer mediaMasterId;
    private Integer primaryRendId;
    
    public void setDisplayRendId(Integer displayRendId) {
        this.displayRendId = displayRendId;
    }
       
    public void setPrimaryRendId(Integer primaryRendId) {
        this.primaryRendId = primaryRendId;
    }
    
    public Integer getMediaMasterId() {
        return mediaMasterId;
    }
    
    public boolean insertNewRecord() {
     
        ResultSet rs = null;
        
        String sql = "insert into MediaMaster " +  
                        "(DisplayRendID, " +
                        "PrimaryRendID, " +
                        "PublicAccess, " +
                        "LoginID, " +
                        "EnteredDate, " +
                        "DepartmentID) " +
                    "values " +
                        "(-1, " +
                        "-1, " +
                        "1 , " +
                        "'CDIS', " +
                        "CURRENT_TIMESTAMP, " +
                        "0)";
        
        logger.log(Level.FINER, "SQL: {0}", sql);
        
        try (Statement stmt = CDIS.getCisConn().createStatement() ) {
            
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.mediaMasterId = rs.getInt(1);
            }    
            else {
                throw new Exception();
            }
            
        } catch (Exception e) {
            logger.log(Level.FINER, "Error: unable to Insert into MediaMaster", e);
            return false;
        
        }finally {
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        
        return true;
    }
    
    public boolean updateRenditionIds() {
        
        int updateCount;
        
        String sql = "update MediaMaster " +
                "set PrimaryRendID = " + this.primaryRendId + ", " +
                "DisplayRendID = " + this.displayRendId + " " +
                "where mediaMasterId = " + getMediaMasterId() ;
        
         logger.log(Level.FINER, "SQL: {0}", sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql)) {
            updateCount = pStmt.executeUpdate(sql);
            
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
