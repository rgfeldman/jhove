/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.identifier;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.database.CdisMap;
import java.sql.PreparedStatement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class RenditionId implements IdentifierType {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String renditionId;
    
    public String getIdentifierCd() {
        return "ren";
    }
    
    public String getIdentifierValue() {
        return renditionId;
    }
    
    public void setIdentifierValue(String identifierValue) {
        renditionId = identifierValue;
    }
    
    public boolean overwriteExistingLinkId() {
        return false;
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        
        CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
        cdisCisIdentifierMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisCisIdentifierMap.setCisIdentifierCd("rnd");
        cdisCisIdentifierMap.populateCisIdentifierValueForCdisMapIdType();
        
        int recordsUpdated;
        //Get the cis
        String sql = "UPDATE mediaRenditions " +
                     "SET isColor = 1 " +
                     "WHERE renditionID = " + cdisCisIdentifierMap.getCisIdentifierValue();
        
        logger.log(Level.FINEST,"SQL! " + sql); 
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql) ) {
            recordsUpdated = pStmt.executeUpdate();
            
            logger.log(Level.FINEST,"Rows Updated in CIS: " + recordsUpdated);
            
            if (recordsUpdated != 1) {
               logger.log(Level.FINER, "Error, no rows in CIS updated");
               throw new Exception();
            }
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error, unable to perform additional update of CIS", e);
                return false;
        }
        
        return true; 
    }
    
}
