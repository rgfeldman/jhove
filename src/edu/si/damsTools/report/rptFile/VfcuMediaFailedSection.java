/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuErrorLog;
import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMediaFailedSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final ArrayList sectionTextData;
     
    public VfcuMediaFailedSection () {
        sectionTextData = new ArrayList<>();
    }
   
    public String returnTitlePhrase() {
        return "Records Generated Errors";
    }
    
    public String returnXmlTag () {  
        return "getFailedRecords";
    }
    
    public String returnEmptyListString() {
        return "There were no Failed Records";
    }
    
    public boolean generateTextForRecord(Integer dataId) {
    
        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(dataId);
        vfcuMediaFile.populateMediaFileName();
        
        VfcuErrorLog vfcuErrorLog = new VfcuErrorLog();
        vfcuErrorLog.setVfcuMediaFileId(dataId);
        
        ArrayList<String> errorDescList = new ArrayList<>();
        errorDescList = vfcuErrorLog.returnDescriptiveInfoForMediaId();
        
        for (String errorDesc : errorDescList ) {
            sectionTextData.add("File: " + vfcuMediaFile.getMediaFileName() + " Error: " + errorDesc) ;
        }

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    
}
