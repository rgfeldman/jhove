/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisObjectMap;
import edu.si.damsTools.cdis.cis.tms.database.MediaRenditions;
import edu.si.damsTools.cdis.cis.tms.database.Objects;
import edu.si.damsTools.cdis.dams.DamsRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */
public class Tms implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final MediaRenditions mediaRendition;
    private final Objects objectTbl;
    
    public Tms() {
        mediaRendition = new MediaRenditions();
        objectTbl = new Objects();
    }
    
    public String getCisImageIdentifier () {
        return mediaRendition.getRenditionId().toString();
    }
    
    public String getGroupIdentifier () {
        return Integer.toString(objectTbl.getObjectId());
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
        
        CdisObjectMap cdisObjectMap = new CdisObjectMap();
        
        cdisObjectMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisObjectMap.populateCisUniqueObjectIdforCdisId();
        
        Objects objects = new Objects();
        objects.setObjectId(Integer.parseInt(cdisObjectMap.getCisUniqueObjectId()) );
        objects.populateObjectNumberForObjectID();
                        
        return "Object: " + objects.getObjectNumber() ;
    }
    
    
    public boolean setBasicValues (String identifier, String uoiId) {
        
        mediaRendition.setRenditionId(Integer.parseInt(identifier));
        boolean objectIdPopuldated = objectTbl.populateMinObjectIDByRenditionId(Integer.parseInt(identifier));
        if (!objectIdPopuldated) {
            return false;
        }
        return true;
    }
    
    public String returnCdisGroupType() {
        return "cdisObjectMap";
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
        
        int recordsUpdated;
        //Get the cis
        String sql = "UPDATE mediaRenditions " +
                     "SET isColor = 1 " +
                     "WHERE renditionID = " + cdisMap.getCisUniqueMediaId();
        
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
    
    public String returnCisUpdateCode() {
        return "CPS";
    } 
}
