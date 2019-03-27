/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.utilities.XmlUtils;

/**
 *
 * @author rfeldman
 */
public class MetaSyncSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<String> sectionTextData;
     
    public MetaSyncSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "Records in DAMS Metadata Synced from the CIS";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In metaDataSync Section"); 
        return "getMetaDataSyncRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no records to Sync";
    }
    
    public boolean generateTextForRecord(Integer recordId) {

        CdisMap cdisMap = new CdisMap();
        cdisMap.setCdisMapId(recordId);
        cdisMap.populateMapInfo();
        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateSiAsstData();
        
        //Put this in until we have multiple source system Ids
        if ( XmlUtils.getConfigValue("lccIdType").equals("ead")) {
            sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Synced with Aspace");
        }
        else {
            sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Synced with " + siAsst.getSourceSystemId());
        }

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
        
}
