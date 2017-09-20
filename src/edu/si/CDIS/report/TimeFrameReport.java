/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.report.attachment.DataSection;
import edu.si.CDIS.report.attachment.FailedSection;
import edu.si.CDIS.report.attachment.MetaSyncSection;
import edu.si.CDIS.report.attachment.MediaCreatedSection;
import edu.si.CDIS.report.attachment.LinkedCisSection;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class TimeFrameReport implements DisplayFormat {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
     public ArrayList<String> returnKeyValueList() {
         return null;
     }
    
    public TimeFrameReport() {
       
    }
        
    
    public String returnDocHeader(String multiRptkeyValue) {
        
        return (CDIS.getProjectCd().toUpperCase() + " CDIS Activity Report- Past " + CDIS.getProperty("rptHours") + " Hours");
                
    }
    
    public String returnEmailToList () {
        return CDIS.getProperty("timeFrameEmailList");
    }

    public String returnEmailTitle(String multiRptkeyValue) {
        return "CDIS Activity Report for " + CDIS.getProjectCd().toUpperCase() + "- Past " + CDIS.getProperty("rptHours") + " Hours";
    }
    
    public boolean returnSupressAttachFlag(String keyValue) {
        
        if ( CDIS.getProperty("tfRptSupressAttch") != null && CDIS.getProperty("vfcuDirRptSupressAttch").equals("true")  ) { 
            return true;
        }
        else {
            return false;
        }
    }
    
    public List<DataSection> sectionFactory() {
        
        List<DataSection> sections = new ArrayList<>();
                
        logger.log(Level.FINEST,"In TF SectionFactory"); 
        
        sections.add(new FailedSection());
        sections.add(new MediaCreatedSection());
        sections.add(new LinkedCisSection());
        sections.add(new MetaSyncSection());
        
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
