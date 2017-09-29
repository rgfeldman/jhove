/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author rfeldman
 */

public class XmlData {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String dataValue;
    private HashMap<String, String> attributes;
    
    public XmlData () {
        attributes = new HashMap<>();
    }
    
    /*public SqlQuery (String query, String type) {
        this.query = query;
        this.type = type;
    }*/
    
    public String getDataValue() {
        return this.dataValue;
    }
    
    public HashMap getAttributes() {
        return this.attributes;
    }
    
    public void setDataValue(String dataValue) {
        this.dataValue = dataValue;
    }
        
    public void addAttribute (String attribute, String value) {
        attributes.put(attribute, value);
    }
    
    public String getCleanDataForAttribute(String attributeType, String attributeValue) {
        
        for (String attribute : attributes.keySet()) {
            if (attribute.equals(attributeType) && attributes.get(attribute).equals(attributeValue)) {
                cleanSqlData();
                return (dataValue);
            }
        }
        return null;
    }
    
    public String getAttributeValue (String attributeType) {
        
        for (String attribute : attributes.keySet()) {
            if (attribute.equals(attributeType) ) {
                return attribute;
            }
        }
        return null;
    }
    
    private void cleanSqlData() {
        if (dataValue.contains("?PROJECT_CD?")) {
            dataValue = dataValue.replace("?PROJECT_CD?", DamsTools.getProjectCd() );
        }
    }
 
}
