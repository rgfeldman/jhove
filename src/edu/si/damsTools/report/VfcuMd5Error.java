/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.report.rptFile.CdisMapFailedSection;
import edu.si.damsTools.report.rptFile.DataSection;
import edu.si.damsTools.report.rptFile.VfcuMd5FailSection;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlUtils;
import edu.si.damsTools.vfcu.database.VfcuMd5FileActivityLog;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuMd5Error implements DisplayFormat{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    
    public String returnDocHeader(String multiRptkeyValue) {
        return "";
    }
    
    public String returnEmailTitle(String multiRptkeyValue) {
       return "Critical Md5 Failure Report" ;
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        return "Statistics for the Report: ";
    }
    
    public boolean returnSuppressAttachFlag(String multiRptkeyValue) {
        return false;
    }
    
    public List<DataSection> sectionFactory() {
        List<DataSection> sections = new ArrayList<>();
                
        logger.log(Level.FINEST,"In TF SectionFactory"); 
                
        if (XmlUtils.returnFirstSqlForTag("getFailedRecords") != null) {
            sections.add(new VfcuMd5FailSection());
        }
        
        return sections;
    }
    
    public boolean updateDbComplete(String multiRptkeyValue) {
        return true;
    }
    
    public boolean returnMultiReportInd() {
        return false;
    }
          
    
}
