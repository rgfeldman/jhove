/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.attachment;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CDISErrorLog;
import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class FailedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private ArrayList sectionTextData;
     
    public FailedSection () {
        sectionTextData = new ArrayList<>();
    }
   
    public String returnTitlePhrase() {
        return "Generated Errors";
    }
    
    public String returnXmlTag () {  
        return "getFailedRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no Failed Records";
    }
    
    public boolean generateTextForRecord(CdisMap cdisMap) {
        
        CDISErrorLog cdisErrorLog = new CDISErrorLog();
        cdisErrorLog.setCdisMapId(cdisMap.getCdisMapId());
        
        String errorDesc = cdisErrorLog.returnDescriptionForMapId();
        sectionTextData.add("File: " + cdisMap.getFileName() + " Error: " + errorDesc) ;

        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
}
