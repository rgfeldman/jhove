/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams;

import edu.si.damsTools.DamsTools;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.dams.database.SiPreservationMetadata;
import edu.si.damsTools.cdis.dams.database.TeamsLinks;
import edu.si.damsTools.cdis.dams.database.FileSizeView;
import edu.si.damsTools.cdis.dams.database.Uois;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.utilities.StringUtils;
import java.util.ArrayList;


/**
 *
 * @author rfeldman
 */
public class DamsRecord {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final Uois uois;
    private final SiAssetMetadata siAsst;
    
    public DamsRecord() {
        uois = new Uois();
        siAsst = new SiAssetMetadata(); 
    }
    
    public SiAssetMetadata getSiAssetMetadata () {
        return siAsst;
    }
    
    public Uois getUois () {
        return uois;
    }
    
    public void setUoiId(String uoiId) {
        uois.setUoiid(uoiId);
        siAsst.setUoiid(uoiId);
    }
    
    public void setBasicData() {
        uois.populateName();
        siAsst.populateSiAsstData();
    } 
    
    public boolean addPreservationData (CdisMap cdisMap) {
        
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(cdisMap.getVfcuMediaFileId());
        vfcuMediaFile.populateVendorChecksum();
        
        // Add the preservation information
        SiPreservationMetadata prev = new SiPreservationMetadata();
        prev.setUoiid(uois.getUoiid());
        prev.setPreservationIdNumber(vfcuMediaFile.getVendorChecksum());
        boolean preservationInserted = prev.insertRow();       
        return preservationInserted;
    }
     
    public String replaceSqlVars (String sql) {
        
        if (sql.contains("?HOLDING_UNIT?")) {
            sql = sql.replace("?HOLDING_UNIT?", getSiAssetMetadata().getHoldingUnit());
        }
        if (sql.contains("?FILE_NAME?")) {
            sql = sql.replace("?FILE_NAME?", getUois().getName());
        }
        if (sql.contains("?BASE_FILE_NAME?")) {
            sql = sql.replace("?BASE_FILE_NAME?", StringUtils.getExtensionlessFileName(getUois().getName()));
        }
        if (sql.contains("?SOURCE_SYSTEM_ID?")) {
            sql = sql.replace("?SOURCE_SYSTEM_ID?", getSiAssetMetadata().getSourceSystemId());
        }
        if (sql.contains("?OWNING_UNIT_UNIQUE_NAME?")) {
            sql = sql.replace("?OWNING_UNIT_UNIQUE_NAME?", getSiAssetMetadata().getOwningUnitUniqueName());
        }
        if (sql.contains("?UOI_ID?")) {
            sql = sql.replace("?UOI_ID?", getUois().getUoiid());
        }
        if (sql.contains("?FILE_SIZE?")) {
            FileSizeView fileSizeView = new FileSizeView();
            fileSizeView.setUoiId(getUois().getUoiid());
            fileSizeView.populateFileSizeInfo();
            sql = sql.replace("?FILE_SIZE?", fileSizeView.getContentSize());
        }
        if (sql.contains("?PIXEL_H?")) {
            sql = sql.replace("?PIXEL_H?", getUois().getBitmapHeight().toString() ) ;
        }
        if (sql.contains("?PIXEL_W?")) {
            sql = sql.replace("?PIXEL_W?", getUois().getBitmapWidth().toString() ) ;
        }
        
        return (sql);
    }
    
    public ArrayList<DamsRecord> returnRelatedDamsRecords () {
        // See if there are any related parent/children relationships in DAMS. We find the parents whether they were put into DAMS
        // by CDIS or not.  We get only the direct parent/child for now...later we may want to add more functionality
                
        ArrayList<DamsRecord> relatedRecordList = new ArrayList<>();
         
        TeamsLinks teamsLinks = new TeamsLinks();
        teamsLinks.setSrcValue(uois.getUoiid());
        teamsLinks.setLinkType("PARENT");
        boolean relatedRecRetrieved = teamsLinks.populateDestValueNotDeleted();
        
        if (relatedRecRetrieved ) {
            DamsRecord childRecord = new DamsRecord();
            childRecord.setUoiId(teamsLinks.getDestValue());
            childRecord.setBasicData();
            relatedRecordList.add(childRecord);
        }
        
        teamsLinks.setLinkType("CHILD");
        relatedRecRetrieved = teamsLinks.populateDestValueNotDeleted();
  
        if (relatedRecRetrieved ) {
            DamsRecord parentRecord = new DamsRecord();
            parentRecord.setUoiId(teamsLinks.getDestValue());
            parentRecord.setBasicData();
            relatedRecordList.add(parentRecord);
        }
        
        return relatedRecordList;
        
    }
    
}
