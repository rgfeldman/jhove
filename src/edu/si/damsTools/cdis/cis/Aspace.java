/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisCisGroupMap;
import edu.si.damsTools.cdis.database.CdisMap;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
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
        return eadRefId;
    }
    
    public String getGroupIdentifier () {
        return null;
    }
    
    public String returnGrpInfoForReport (CdisMap cdisMap) {
                        
        return null;
    }
    
    public boolean setBasicValues (String identifier, String uoiId) {
        SiAssetMetadata siAsst = new SiAssetMetadata();
        siAsst.setUoiid(uoiId);
        siAsst.populateEadRefId();
        eadRefId = siAsst.getEadRefId();
        return true;
    }

    public String returnCdisGroupType() {
        return null;
    }
    
    public boolean additionalCisUpdateActivity(DamsRecord damsRecord, CdisMap notUsed) {
                
        ArrayList<CdisMap> cdisMapList = new ArrayList();
        cdisMapList = returnMappedIdsForEad(damsRecord);
        
        for (CdisMap cdisMap : cdisMapList) {
            
            //See if this row has eadRefId Already
            CdisCisGroupMap cdisCisGroup = new CdisCisGroupMap();
            cdisCisGroup.setCdisMapId(cdisMap.getCdisMapId());
            cdisCisGroup.setCisGroupValue(this.eadRefId);
            cdisCisGroup.setCisGroupCd("ead");
            cdisCisGroup.populateIdForCdisMapID();
            
            if (cdisCisGroup.getCdisCisGroupMapId() == null) {
                // insert a new group id
                boolean cisGroupCreated = cdisCisGroup.createRecord();
                if (! cisGroupCreated) {
                    logger.log(Level.FINEST,"Unable to create group record! "); 
                    return false;
                }
            }
            else {
                //update the new group id
                cdisCisGroup.updateCisGroupValue();
            }
        }
       
        return true;
    }
    
    private ArrayList returnMappedIdsForEad(DamsRecord damsRecord) {
        
        ArrayList<CdisMap> cdisMapList = new ArrayList();
        
         //Get all the CDIS records that are linked to DAMS that have the given eadRefId;
        String sql = "SELECT cdm.cdis_map_id " +
                      "FROM cdis_map cdm " +
                      "INNER JOIN towner.si_asset_metadata sim " +
                      "ON cdm.dams_uoi_id = sim.uoi_id " +
                      "WHERE sim.ead_ref_id = '" + damsRecord.getSiAssetMetadata().getEadRefId() + "'";
         
        logger.log(Level.FINEST,"SQL! " + sql); 
        try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            while (rs != null && rs.next()) {
                CdisMap cdisMap = new CdisMap();
                cdisMap.setCdisMapId(rs.getInt(1));
                cdisMapList.add(cdisMap);
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain EadRefId from cdis_map", e );
                return null;
        } 
        return cdisMapList;
    }
    
    public String returnCisUpdateCode() {
        return "CPS";
    } 
    
}
