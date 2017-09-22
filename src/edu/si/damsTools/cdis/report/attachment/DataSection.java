/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.report.attachment;

import edu.si.damsTools.cdis.database.CDISMap;
import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public interface DataSection {
    
    public String returnTitlePhrase();
    
    public String returnXmlTag();
    
    public String returnEmptyListString();
    
    public boolean generateTextForRecord(CDISMap cdisMap);
    
    public ArrayList getSectionTextData();
           
}
