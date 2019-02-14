/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.utilities;

import edu.si.damsTools.DamsTools;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class DbUtils {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());

    
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
