/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.cdis.database.CdisMap;
import edu.si.damsTools.cdis.database.CdisErrorLog;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class CdisMapFailedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final ArrayList sectionTextData;
     
    public CdisMapFailedSection () {
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
    
    public boolean generateTextForRecord(Integer dataId) {
           
        CdisMap cdisMap = new CdisMap();
        cdisMap.setCdisMapId(dataId);
        cdisMap.populateFileName();
        
        CdisErrorLog cdisErrorLog = new CdisErrorLog();
        cdisErrorLog.setCdisMapId(dataId);
        
        ArrayList<String> errorDescList = new ArrayList<>();
        errorDescList = cdisErrorLog.returnDescriptionsForMapId();
        
        for (String errorDesc : errorDescList ) {
            sectionTextData.add("File: " + cdisMap.getFileName() + " Error: " + errorDesc) ;       
        }
        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
    private static void printList(ArrayList<?> list) {
        
    }
}
