/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.report.rptFile.DataSection;
import edu.si.damsTools.report.rptFile.CdisMapFailedSection;
import edu.si.damsTools.report.rptFile.MetaSyncSection;
import edu.si.damsTools.report.rptFile.MediaCreatedSection;
import edu.si.damsTools.report.rptFile.LinkedCisSection;
import edu.si.damsTools.report.rptFile.LinkedDamsSection;
import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlData;
import edu.si.damsTools.utilities.XmlUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TimeFrameReport implements DisplayFormat {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public TimeFrameReport() {
       
    }
        
    
    public String returnDocHeader(String multiRptkeyValue) {
        
        return (XmlUtils.getConfigValue("projectCd").toUpperCase() + " CDIS Activity Report- Past " + XmlUtils.getConfigValue("rptHours") + " Hours");
                
    }

    public String returnEmailTitle(String multiRptkeyValue) {
        return "CDIS Activity Report for " + XmlUtils.getConfigValue("projectCd").toUpperCase() + "- Past " + XmlUtils.getConfigValue("rptHours") + " Hours";
    }
    
    public boolean returnSuppressAttachFlag(String keyValue) {
        
        return XmlUtils.getConfigValue("suppressAttch").equals("true");
    }
    
    public List<DataSection> sectionFactory() {
        
        List<DataSection> sections = new ArrayList<>();
                
        logger.log(Level.FINEST,"In TF SectionFactory"); 
        
        sections.add(new CdisMapFailedSection());
          
        if (XmlUtils.returnFirstSqlForTag("getCisMediaCreatedRecords") != null) {
            sections.add(new MediaCreatedSection());
        }
        if (XmlUtils.returnFirstSqlForTag("getDamsLinkedRecords") != null) {
            sections.add(new LinkedDamsSection());
        }
        if (XmlUtils.returnFirstSqlForTag("getCisLinkedRecords") != null) { 
            sections.add(new LinkedCisSection());
        }
        if (XmlUtils.returnFirstSqlForTag("getMetaDataSyncRecords") != null) {
            sections.add(new MetaSyncSection());
        }
        
        return sections;
    }
    
    public boolean updateDbComplete (String multiRptkeyValue) {
        return true;
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        return "Statistics For the Report:";
    }
    
    public boolean returnMultiReportInd() {
        return false;
    }
                 
}
