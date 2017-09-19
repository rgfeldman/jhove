/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report.attachment;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.DAMS.Database.SiAssetMetadata;
import edu.si.CDIS.Database.CDISMap;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class LinkedDamsSection implements DataSection {
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private ArrayList<String> sectionTextData;
     
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
    
    public boolean generateTextForRecord(CDISMap cdisMap) {
        
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
