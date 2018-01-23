/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.rptFile;

import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        return "in DAMS Metadata Synced from the CIS";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In metaDataSync Section"); 
        return "getMetaDataSyncRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no records to Sync";
    }
    
    public boolean generateTextForRecord(CdisMap cdisMap) {

        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        CisRecordFactory cisFactory = new CisRecordFactory();
        CisRecordAttr cisAttr = cisFactory.cisChooser();
        
        sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Synced with " + cisAttr.returnGrpInfoForReport(cdisMap));

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
        
}
