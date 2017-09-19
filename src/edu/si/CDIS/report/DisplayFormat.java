/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.report;

import edu.si.CDIS.report.attachment.DataSection;
import java.util.List;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public interface DisplayFormat  {
    
    //public void generate();
    
    public String returnDocHeader(String multiRptkeyValue);
    
    public String returnEmailToList();
    
    public String returnEmailTitle(String multiRptkeyValue);
    
    public boolean returnSupressAttachFlag(String multiRptkeyValue);
    
    public List<DataSection> sectionFactory();
    
    public boolean populateMultiReportKeyValues();
    
    public ArrayList<String> returnKeyValueList();
    
    public boolean updateDbComplete(String multiRptkeyValue);
        
}
