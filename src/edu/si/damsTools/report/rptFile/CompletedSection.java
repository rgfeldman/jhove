/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.vfcu.database.VfcuMediaFile;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public class CompletedSection implements DataSection {
    
    private final ArrayList sectionTextData;
    
    public CompletedSection () {
        sectionTextData = new ArrayList<>();
    }
    
    public String returnTitlePhrase() {
        return "Files completed through VFCU";
    }
    
    public String returnXmlTag () {  
        return "getVfcuComplete";
    }
    
    public String returnEmptyListString() {
        return "There were no Completed Records";
    }
    
    public boolean generateTextForRecord(Integer recordId) {

        VfcuMediaFile vfcuMediaFile = new VfcuMediaFile();
        vfcuMediaFile.setVfcuMediaFileId(recordId);
        vfcuMediaFile.populateMediaFileName();
        vfcuMediaFile.populateMediaFileAttr();
        
        sectionTextData.add("File: " + vfcuMediaFile.getMediaFileName()) ;

        return true;
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
}
