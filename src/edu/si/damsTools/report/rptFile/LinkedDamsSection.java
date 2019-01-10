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

/**
 *
 * @author rfeldman
 */
public class LinkedDamsSection implements DataSection {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final ArrayList<String> sectionTextData;
     
    public LinkedDamsSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "Successfully Validated and Linked in DAMS";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In DamsLinked Section"); 
        return "getDamsLinkedRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no records to Link";
    }
    
    public boolean generateTextForRecord(Integer recordId) {
        
        CdisMap cdisMap = new CdisMap();
        cdisMap.setCdisMapId(recordId);
        cdisMap.populateMapInfo();
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        sectionTextData.add("File: " + cdisMap.getFileName() + "  DAMS UAN: " + siAsst.getOwningUnitUniqueName());
        
        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
}
