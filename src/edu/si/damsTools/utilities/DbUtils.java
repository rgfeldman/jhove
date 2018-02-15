/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.sql.Connection;

/**
 *
 * @author rfeldman
 */
public class DbUtils {
    
    public static Connection returnDbConnFromString (String db) {
        
        if (db!= null) {
            switch (db) {
                case "dams" :
                    return DamsTools.getDamsConn();
                case "cis" :
                    return DamsTools.getCisConn();  
            }
        }
        return null;
    }
}
