/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.util.logging.Level;

/**
 *
 * @author rfeldman
 */
public class XmlUtils {
        
    // Method : returnFirstSqlForTag
    // Purpose : Loops through the entire xml query structure and 
    //              returns the first query (data associated with the xml tag 'query'
    //              that matches the supplied tag  
    public static String returnFirstSqlForTag(String tag) {
        String sql = null;
        for(XmlData xmlInfo : DamsTools.getXmlQueryDataList()) {
            if (! xmlInfo.getTag().equals("query")) {
                continue;
            }
            sql = xmlInfo.getDataValuesForAttribute("type",tag);
            if (sql != null) {
                break;
            }
        }
        return sql;
    }
    
    // Method : getConfigValue
    // Purpose : Loops through the entire xml config structure and 
    //              returns the first data associated with the xml tag specified  
    public static String getConfigValue (String tag) {
        String data = null;
        for(XmlData xmlInfo : DamsTools.getXmlConfigDataList()) {
            if (! xmlInfo.getTag().equals(tag)) {
                continue;
            }
            data = xmlInfo.getDataValue();
            if (data != null) {
                break;
            }
        }
        return data;
    }

}
