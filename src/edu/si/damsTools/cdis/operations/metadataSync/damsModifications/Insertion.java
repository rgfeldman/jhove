/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync.damsModifications;

import java.util.regex.Pattern;
import edu.si.damsTools.cdis.operations.metadataSync.MetadataColumnData;

/**
 *
 * @author rfeldman
 */
public class Insertion extends ModificationsForDams {
    public Insertion(String uoiId, String tableName) {
        super(uoiId, tableName);
    }
    
    public void populateSql(String columnName, String valuesToInsert) {
        sql = "INSERT INTO towner." + tableName + 
            " (UOI_ID, " + columnName + ") VALUES ('" + 
                uoiId + "','" + valuesToInsert + "')"; 
    }
    
    public void appendToExistingSql(String columnName, String dataValue) {

       sql = sql.replace(") VALUES (", ", columnName ) VALUES");
       sql = sql.replaceAll(")$","," + dataValue + ")");
       
    }
        
}
