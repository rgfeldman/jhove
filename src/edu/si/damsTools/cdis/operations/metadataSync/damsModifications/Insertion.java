/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync.damsModifications;

import edu.si.damsTools.DamsTools;
import java.util.regex.Pattern;
import edu.si.damsTools.cdis.operations.metadataSync.MetadataColumnData;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class Insertion extends ModificationsForDams {
    public Insertion(String uoiId, String tableName) {
        super(uoiId, tableName);
    }
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    public void populateSql(String columnName, String valuesToInsert) {
        sql = "INSERT INTO towner." + tableName + 
            " (UOI_ID, " + columnName + ") VALUES ('" + 
                uoiId + "','" + valuesToInsert + "')"; 
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
    }
    
    
    
    public void appendToExistingSql(String columnName, String dataValue) {

        sql = sql.replace(") VALUES (", ", " + columnName + " ) VALUES (");
        
        logger.log(Level.FINEST,"columnName! " + columnName);
        logger.log(Level.FINEST,"dataValue! " + dataValue);
        logger.log(Level.FINEST,"SQL! " + sql);
        
        sql = sql.replaceAll("\\)$",", '" + dataValue );
        sql = sql + "')";
       
        logger.log(Level.FINEST,"SQL! " + sql);
       
    }
        
}
