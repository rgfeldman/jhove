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
import edu.si.damsTools.vfcu.database.VfcuMd5File;

/**
 *
 * @author rfeldman
 */
public class VfcuMd5InProcess implements DataSection {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final ArrayList sectionTextData;
     
    public VfcuMd5InProcess () {
        sectionTextData = new ArrayList<>();
    }
   
    public String returnTitlePhrase() {
        return "Currently Processing or Incomplete";
    }
    
    public String returnXmlTag () {  
        return "getMd5InProcess";
    }
    
    public String returnEmptyListString() {
        return "There were no MD5 Files Picked up by VFCU";
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
