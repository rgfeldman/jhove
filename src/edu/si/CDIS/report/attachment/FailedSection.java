/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report.attachment;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.Database.CDISMap;
import edu.si.CDIS.Database.CDISErrorLog;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class FailedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
   
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
    
    public boolean generateTextForRecord(CDISMap cdisMap) {
        
        CDISErrorLog cdisErrorLog = new CDISErrorLog();
        cdisErrorLog.setCdisMapId(cdisMap.getCdisMapId());
        
        String errorDesc = cdisErrorLog.returnDescriptionForMapId();
        sectionTextData.add("File: " + cdisMap.getFileName() + "Error: " + errorDesc) ;

        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
}
