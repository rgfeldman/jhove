/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.cis.rdms.Rdms;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CdisCisMdsPullLog {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String lastPullDt;
    private Integer cdisCisMdsPullLogId;
    
    public String getLastPullDt(){
        return this.lastPullDt;
    }
    
    public void setLastPullDt(String lastPullDt) {
        this.lastPullDt = lastPullDt;
    }
    
    private void populateIdFromConfigValues () {
        String sql = "SELECT cdis_cis_mds_pull_log_id " +
                    "FROM cdis_cis_mds_pull_log " + 
                    "WHERE project_cd = '" + XmlUtils.getConfigValue("projectCd") + "' " +
                    "AND cis_name = '" + XmlUtils.getConfigValue("cis") + "' " +
                    "AND cis_instance = '" + XmlUtils.getConfigValue("cisInstance") + "'" ;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.cdisCisMdsPullLogId = rs.getInt(1);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain ID of previous run", e );
        }
    }
    
    private void updatePullDtCurrent () {
        
        int rowsUpdated = 0;
        
        String sql = "UPDATE cdis_cis_mds_pull_log " +
                     "SET last_pull_dt = TO_DATE ('" + this.lastPullDt + "', 'YYYY-MM-DD HH24:MI:SS') " +
                     "WHERE cdis_cis_mds_pull_log_id = " + this.cdisCisMdsPullLogId;
                   
        
        logger.log(Level.FINER, "SQL: " + sql );
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql)) {
            rowsUpdated = pStmt.executeUpdate(); 
            
             if (rowsUpdated != 1) {
                throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to insert CDIS_activity_log table", e );
        }
    }
    
    
    public void updateOrInsertPullDt() {
        
        populateIdFromConfigValues();
        if (cdisCisMdsPullLogId != null) {
            updatePullDtCurrent();
        }
        else {
            createRow();
        }    
    }    
    
    private boolean createRow() {
        int rowsInserted = 0;
        
        String sql = "INSERT INTO cdis_cis_mds_pull_log ( " +
                        "cdis_cis_mds_pull_log_id, " +
                        "project_cd, " +
                        "cis_name, " +
                        "cis_instance, " +
                        "last_pull_dt) " +
                    "values ( " + 
                        "cdis_cis_mds_pull_log_id_seq.NextVal, '" +
                        XmlUtils.getConfigValue("projectCd") + "', '" + 
                        XmlUtils.getConfigValue("cis") + "', '" +
                        XmlUtils.getConfigValue("cisInstance") + "', " +
                        "TO_DATE ('" + this.lastPullDt + "', 'YYYY-MM-DD HH24:MI:SS'))";
        
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
    
    public boolean populateLastMdsDateTime(Rdms cisRdms) {
        
        String sql = "SELECT last_pull_dt " +
                    "FROM cdis_cis_mds_pull_log " +
                    "WHERE project_cd  = '" + XmlUtils.getConfigValue("projectCd") + "'" +
                    " AND cis_name = '" + XmlUtils.getConfigValue("cis") + "'" + 
                    " AND cis_instance = '" + XmlUtils.getConfigValue("cisInstance") + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
               ResultSet rs = pStmt.executeQuery()) {
            
            if (rs.next()) {
                this.lastPullDt = rs.getString(1);
            }   
            else {
                this.lastPullDt = cisRdms.returnDbDateTime(-1);
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain date of previous run", e );
                return false;
        }
        
        return true;
    }
}
