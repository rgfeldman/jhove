/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report.attachment;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.CIS.CisAttr;
import edu.si.CDIS.CIS.CisFactory;
import edu.si.CDIS.DAMS.Database.SiAssetMetadata;
import edu.si.CDIS.Database.CDISMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class LinkedCisSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
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
    
    public boolean generateTextForRecord(CDISMap cdisMap) {
        
        SiAssetMetadata siAsst = new SiAssetMetadata();
        
        siAsst.setUoiid(cdisMap.getDamsUoiid());
        siAsst.populateOwningUnitUniqueName();
        
        CisFactory cisFactory = new CisFactory();
        CisAttr cisAttr = cisFactory.cisChooser();
        
        sectionTextData.add("UAN: " + siAsst.getOwningUnitUniqueName() + " Linked To " + cisAttr.returnGrpInfoForReport(cdisMap));

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
       
}
