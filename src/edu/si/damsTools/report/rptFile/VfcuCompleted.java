/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import java.util.ArrayList;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuCompleted implements DataSection{
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final ArrayList sectionTextData;
     
    public VfcuCompleted () {
        sectionTextData = new ArrayList<>();
    }
   
    public String returnTitlePhrase() {
        return "Are VFCU Completed Directories in Past " + XmlUtils.getConfigValue("rptHours") + " Hours";
    }
    
    public String returnXmlTag () {  
        return "getCompletedMd5s";
    }
    
    public String returnEmptyListString() {
        return "There were no newly completed MD5 Files";
    }
    
    public boolean generateTextForRecord(Integer dataId) {
    
        VfcuMd5File vfcuMd5FileId = new VfcuMd5File();
        vfcuMd5FileId.setVfcuMd5FileId(dataId);
        vfcuMd5FileId.populateBasicDbData();
        
        sectionTextData.add("Md5 File: " + vfcuMd5FileId.getFilePathEnding()+ "/" + vfcuMd5FileId.getVendorMd5FileName());

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
}
