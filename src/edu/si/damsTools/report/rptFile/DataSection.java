/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.report.rptFile;

import java.util.ArrayList;

/**
 *
 * @author rfeldman
 */
public interface DataSection {
    
    public String returnTitlePhrase();
    
    public String returnXmlTag();
    
    public String returnEmptyListString();
    
    public boolean generateTextForRecord(Integer recordId);
    
    public ArrayList getSectionTextData();
           
}
