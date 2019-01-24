/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report;

import edu.si.damsTools.report.rptFile.DataSection;
import java.util.List;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public interface DisplayFormat  {
        
    public String returnDocHeader(String multiRptkeyValue);
    
    public String returnEmailTitle(String multiRptkeyValue);
           
    public String returnStatsListsHeader(String multiRptkeyValue);
    
    public boolean returnSuppressAttachFlag(String multiRptkeyValue);
    
    public boolean returnMultiReportInd();
    
    public List<DataSection> sectionFactory();
    
    public boolean updateDbComplete(String multiRptkeyValue);
    
}
