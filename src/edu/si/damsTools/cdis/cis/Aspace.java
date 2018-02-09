/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.database.CdisActivityLog;
import edu.si.damsTools.cdis.database.CdisCisIdentifierMap;
import edu.si.damsTools.cdis.database.CdisMap;
import java.util.ArrayList;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class Aspace implements CisRecordAttr {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String eadRefId;
    
    public String getCisImageIdentifier () {
        return null;
    }
    
    public String getGroupIdentifier () {
        return eadRefId;
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {                
        return null;
    }
    
    public boolean setBasicValues (String identifier, DamsRecord damsRecord) {
        eadRefId = damsRecord.getSiAssetMetadata().getEadRefId();
        return true;
    }

    public String returnCdisGroupType() {
        return "un";
    }
     
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap cdisMap) {
                
        //get List of other cdisMapIds that may be on the same RefId (we only send one         
        CdisCisIdentifierMap cdisCisIdentifierMap = new CdisCisIdentifierMap();
        cdisCisIdentifierMap.setCdisMapId(cdisMap.getCdisMapId());
        cdisCisIdentifierMap.setCisIdentifierCd("ead");
        cdisCisIdentifierMap.setCisIdentifierValue(damsRecord.getSiAssetMetadata().getEadRefId());
        ArrayList<Integer> cdisMapIds = cdisCisIdentifierMap.returnCdisMapIdsForCisCdValue();

        for (Integer mapId : cdisMapIds) {
            //Add the other mapids as CPD, all of them should be 
            if (!Objects.equals(mapId, cdisMap.getCdisMapId())) {
                logger.log(Level.FINER, "updating mapID " + mapId + " origMapId: " + cdisMap.getCdisMapId()  );
                
                CdisActivityLog cdisActivity = new CdisActivityLog();
                cdisActivity.setCdisMapId(mapId);
                cdisActivity.setCdisStatusCd("CPD");
        
                cdisActivity.updateOrInsertActivityLog();         
            }
       }
       
        return true;
    }
   
    public String returnCisUpdateCode() {
        return "CPD";
    } 
    
}
