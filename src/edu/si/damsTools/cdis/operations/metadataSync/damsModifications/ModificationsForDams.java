/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync.damsModifications;

import edu.si.damsTools.DamsTools;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class ModificationsForDams {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    protected final String tableName;
    protected String sql;
    protected final String uoiId;
    
    public String getTableName() {
        return this.tableName;
    }
    
    public String getSql() {
        return this.sql;
    }
    
    public void setSql(String sql) {
        this.sql = sql;
    }
     
    
    public ModificationsForDams(String uoiId, String tableName) {
        this.tableName = tableName;
        this.uoiId = uoiId;
    }

     /*  Method :        updateDamsData
        Arguments:      
        Description:    Updates the DAMS with the metadata changes 
        RFeldman 2/2015
    */
    public int updateDamsData() {

        int recordsUpdated;
        
        setSql(getSql().replaceAll("null", ""));
                
        logger.log(Level.FINEST,"SQL TO UPDATE: " + getSql());
        
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(getSql())) {
 
            recordsUpdated = pStmt.executeUpdate(getSql());
            
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
            logger.log(Level.FINEST,"Error updating DAMS data", e);
            return -1;    
        } 
        
        return recordsUpdated;
    }
}
