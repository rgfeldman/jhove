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
    
     public ArrayList<String> returnKeyValueList() {
         return null;
     }
    
    public TimeFrameReport() {
       
    }
        
    
    public String returnDocHeader(String multiRptkeyValue) {
        
        return (DamsTools.getProjectCd().toUpperCase() + " CDIS Activity Report- Past " + DamsTools.getProperty("rptHours") + " Hours");
                
    }
    
    public String returnEmailToList () {
        return DamsTools.getProperty("timeFrameEmailList");
    }

    public String returnEmailTitle(String multiRptkeyValue) {
        return "CDIS Activity Report for " + DamsTools.getProjectCd().toUpperCase() + "- Past " + DamsTools.getProperty("rptHours") + " Hours";
    }
    
    public boolean returnSupressAttachFlag(String keyValue) {
        
        return DamsTools.getProperty("tfRptSupressAttch") != null && DamsTools.getProperty("tfRptSupressAttch").equals("true");
    }
    
    public List<DataSection> sectionFactory() {
        
        List<DataSection> sections = new ArrayList<>();
                
        logger.log(Level.FINEST,"In TF SectionFactory"); 
        
        sections.add(new CdisMapFailedSection());
        
        for(XmlData xmlInfo : DamsTools.getSqlQueryObjList()) {
            
            if (xmlInfo.getDataAttributeForTag("query","type","getCisMediaCreatedRecords") != null) {
                sections.add(new MediaCreatedSection());
            }
            if (xmlInfo.getDataAttributeForTag("query","type","getDamsLinkedRecords") != null) {
                sections.add(new LinkedDamsSection());
            }
            if (xmlInfo.getDataAttributeForTag("query","type","getCisLinkedRecords") != null) { 
                sections.add(new LinkedCisSection());
            }
            if (xmlInfo.getDataAttributeForTag("query","type","getMetaDataSyncRecords") != null) {
                sections.add(new MetaSyncSection());
            }
        }
        
        return sections;
    }
    
    
    public boolean populateMultiReportKeyValues() {
       
        return false;
    }
    
    public boolean updateDbComplete (String multiRptkeyValue) {
        return true;
    }
    
    public String returnStatsListsHeader(String multiRptkeyValue) {
        return "Statistics For the Report:";
    }
           
}
