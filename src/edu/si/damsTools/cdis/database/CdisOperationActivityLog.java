/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisOperationActivityLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    String operationActivityCd;
    String activityDt;
    
    public String getActivityDt(){
        return this.activityDt;
    }
    
    public String getOperationActivityCd () {
        return this.operationActivityCd;
    }
    
    public void setActivityDt(String activityDt) {
        this.activityDt = activityDt;
    }
    
    public void setprocessActivityCd(String operationActivityCd) {
        this.operationActivityCd = operationActivityCd;
    }
    
    public boolean createRecord() {
        int rowsInserted = 0;
        
        String sql = "INSERT INTO cdis_operation_activity_log ( " +
                        "cdis_operation_activity_log_id, " +
                        "project_cd, " +
                        "operation_type, " +
                        "operation_activity_cd, " +
                        "activity_dt) " +
                    "values ( " + 
                        "cdis_operation_activity_id_seq.NextVal, '" +
                        XmlUtils.getConfigValue("projectCd") + "', '" + 
                        DamsTools.getOperationType() + "', '" +
                        this.operationActivityCd + "', " +
                        "TO_DATE ('" + this.activityDt + "', 'YYYY-MM-DD HH24:MI:SS'))";
        
        logger.log(Level.FINER, "SQL: " + sql );
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            rowsInserted = pStmt.executeUpdate(); 
            
             if (rowsInserted != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert CDIS_activity_log table", e );
                return false;
        }
        return true;
    }
    
    public String returnMaxDateTime() {
        String maxDateTime = null;
        
        String sql = "SELECT NVL(MAX(activity_dt),SYSDATE) " +
                    "FROM cdis_operation_activity_log " +
                    "WHERE project_cd  = '" + XmlUtils.getConfigValue("projectCd") + "'" +
                    " AND operation_type = '" + DamsTools.getOperationType() + "' ";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                maxDateTime = rs.getString(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain date of previous run", e );
        }
        
        return maxDateTime;
    }
}
