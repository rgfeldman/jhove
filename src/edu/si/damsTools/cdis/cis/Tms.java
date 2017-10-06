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
        return mediaRendition.getRenditionNumber();
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
    
    
    public boolean setBasicValues (String cisRecordId) {
        mediaRendition.setRenditionId(Integer.parseInt(cisRecordId));
        boolean objectIdPopuldated = objectTbl.populateMinObjectIDByRenditionId(Integer.parseInt(cisRecordId));
        if (!objectIdPopuldated) {
            return false;
        }
        return true;
    }
    
    public String returnCdisGroupType() {
        return "cdisObjectMap";
    }
    
}
