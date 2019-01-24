/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.vfcu.database.VfcuMd5FileError;
import edu.si.damsTools.vfcu.database.VfcuMd5File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMd5FailSection implements DataSection {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
   
    private final ArrayList sectionTextData;
     
    public VfcuMd5FailSection () {
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
                   
        VfcuMd5File vfcuMd5File = new VfcuMd5File();
        vfcuMd5File.setVfcuMd5FileId(dataId);
        vfcuMd5File.populateBasicDbData();
        
        VfcuMd5FileError vfcuMd5ErrorLog = new VfcuMd5FileError();
        vfcuMd5ErrorLog.setVfcuMd5FileId(dataId);
        
        ArrayList<String> errorDescList = new ArrayList<>();
        errorDescList = vfcuMd5ErrorLog.returnDescriptionsForId();
        
        for (String errorDesc : errorDescList ) {
            sectionTextData.add("Md5 File: " + vfcuMd5File.getFilePathEnding()+ "/" + vfcuMd5File.getVendorMd5FileName() + " Error: " + errorDesc) ;
        }

        return true;
        
    }
    
    public ArrayList getSectionTextData () {
        return this.sectionTextData;
    }
    

}
