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
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MediaCreatedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private ArrayList<String> sectionTextData;
     
    public MediaCreatedSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "Created in the CIS and Linked by CDIS";
    }
    
    public String returnXmlTag () {
        logger.log(Level.FINEST,"In mediaCreated Section"); 
        return "getCisMediaCreatedRecords";
    }
    
    public String returnEmptyListString() {
        return "There was no new media Created in the CIS";
    }
    
    public boolean generateTextForRecord(CdisMap cdisMap) {
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        CisRecordFactory cisFactory = new CisRecordFactory();
        CisRecordAttr cisAttr = cisFactory.cisChooser();
        
        sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Created On " + cisAttr.returnGrpInfoForReport(cdisMap));
        
        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
        
}
