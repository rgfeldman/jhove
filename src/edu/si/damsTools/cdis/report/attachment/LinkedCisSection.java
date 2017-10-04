/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.attachment;

import edu.si.damsTools.cdis.cis.CisRecordAttr;
import edu.si.damsTools.cdis.cis.CisRecordFactory;
import edu.si.damsTools.cdis.dams.database.SiAssetMetadata;
import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class LinkedCisSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList sectionTextData;
     
    public LinkedCisSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "in DAMS Linked to a prior-Existing CIS Record";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In CisLinked Section");  
        return "getCisLinkedRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no records to Link";
    }
    
    public boolean generateTextForRecord(CdisMap cdisMap) {
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        CisRecordFactory cisFactory = new CisRecordFactory();
        CisRecordAttr cisAttr = cisFactory.cisChooser();
        
        sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Linked To " + cisAttr.returnGrpInfoForReport(cdisMap));

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
       
}
