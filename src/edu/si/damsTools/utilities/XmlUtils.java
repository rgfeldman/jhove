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
        
    public static String returnFirstSqlForTag(String tag) {
        String sql = null;
        for(XmlData xmlInfo : DamsTools.getSqlQueryObjList()) {
            if (! xmlInfo.getTag().equals("query")) {
                continue;
            }
            sql = xmlInfo.getDataValuesForAttribute("type",tag);
            if (sql != null) {
                break;
            }
        }
        return (sql);
    }

}
