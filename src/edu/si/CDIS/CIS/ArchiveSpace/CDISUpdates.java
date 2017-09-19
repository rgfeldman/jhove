/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.ArchiveSpace;

import edu.si.CDIS.CDIS;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class CDISUpdates {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
     
    private String holdingUnit;
    private String eadRefId;
     
    public String getEadRefId () {
        return this.eadRefId;
    }
     
    public String getHoldingUnit() {
        return this.holdingUnit;
    }
     
    public void setEadRefId (String eadRefId) {
        this.eadRefId = eadRefId;
    }
     
    public void setHoldingUnit (String holdingUnit) {
         this.holdingUnit = holdingUnit;
    }
     
    public boolean createRecord () {
         String sql = "INSERT INTO CDIS_updates (" +
                        "ead_ref_id, " +
                        "DAMSingestDate, " +
                        "action, " + 
                        "holding_unit, " +
                        "dept)" +
                      "VALUES ( " +
                        "'" + getEadRefId() + "', " +
                        "CURDATE(), " +
                        "'new', " +
                        "'" + getHoldingUnit() +  "', " +
                        "'archives')";
         
        logger.log(Level.FINEST,"SQL for update in Archive space:" + sql );
             
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql)) {
        
            int rowsUpdated = pStmt.executeUpdate();
            
            if (rowsUpdated != 1) {
                throw new Exception();
            }      
            
       } catch (Exception e) {
               logger.log(Level.FINER, "Error: unable to insert into CDIS_OBJECT_MAP table", e );
               return false;
       }      
       
        return true;
     }
     
   
     
    
    public boolean callGetDescriptiveData () {
       
        String sql = "CALL getDescriptiveData (\"" + getEadRefId() + "\")";
        
        logger.log(Level.FINEST,"SQL:" + sql );
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql)) {
            //This only needs to happen in one place.  It gets set every time here which is not needed.
            
            pStmt.executeQuery();
       
        } catch (Exception e) {
               logger.log(Level.FINER, "Error in getDescriptiveDate", e );
               return false;
        } 
       
       return true; 
                 
     }
}
