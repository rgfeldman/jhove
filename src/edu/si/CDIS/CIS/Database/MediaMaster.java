/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.DataProvider;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author rfeldman
 */
public class MediaMaster {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    Integer mediaMasterID;
    
    public Integer getMediaMasterID () {
        return mediaMasterID;
    }
    
    public void insertNewRecord(Connection cisConn) {
     
        Statement stmt = null;
        
        // These are the values from ANACOSTIA database:
        //DepartmentID = 0 for FSG, null for ACM, 0 for CHSDM, 0 for NASM, 0 for NMAAHC
        //copyright in NMAAHC, 
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
        
        //--update MediaMaster.PrimaryRendID, DisplayRendID
	//update MediaMaster set PrimaryRendID = (select top 1 id from @renditionIDs), 
	//						DisplayRendID = (select top 1 id from @renditionIDs)
	//where MediaMasterID = (select top 1 id from @masterIDs);
        
        try {
            stmt = cisConn.createStatement();
            stmt.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);

            ResultSet rs = stmt.getGeneratedKeys();
            if (rs != null && rs.next()) {
                this.mediaMasterID = rs.getInt(1);
            }    
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (stmt != null) stmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
                
        logger.log(Level.FINER, "SQL: {0}", sql);
    }
    
}
