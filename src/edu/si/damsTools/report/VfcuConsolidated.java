/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.report.rptFile.DataSection;
import edu.si.damsTools.report.rptFile.VfcuMd5FailSection;
import edu.si.damsTools.report.rptFile.VfcuMd5InProcess;
import edu.si.damsTools.report.rptFile.VfcuCompleted;
import edu.si.damsTools.utilities.XmlUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class VfcuConsolidated implements DisplayFormat{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    
    public String returnDocHeader(String multiRptkeyValue) {
        return "Consolidated VFCU Progress Report - Past " + XmlUtils.getConfigValue("rptHours") + " Hours";
    }
    
    public String returnEmailTitle(String multiRptkeyValue) {
       return "Consolidated VFCU Progress Report Past " + XmlUtils.getConfigValue("rptHours") + " Hours";
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        return "Statistics for the Report: ";
    }
    
    public boolean returnSuppressAttachFlag(String multiRptkeyValue) {
        return false;
    }
    
    public List<DataSection> sectionFactory() {
        List<DataSection> sections = new ArrayList<>();
                
        logger.log(Level.FINEST,"In Watcher SectionFactory"); 
                
        if (XmlUtils.returnFirstSqlForTag("getFailedRecords") != null) {
            sections.add(new VfcuMd5FailSection());
        }
        if (XmlUtils.returnFirstSqlForTag("getMd5InProcess") != null) {
            sections.add(new VfcuMd5InProcess());
        }
        if (XmlUtils.returnFirstSqlForTag("getCompletedMd5s") != null) {
            sections.add(new VfcuCompleted());
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
