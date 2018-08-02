/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync.damsModifications;

/**
 *
 * @author rfeldman
 */
public class Update extends ModificationsForDams {
    
     //never update these special fields in parent/child sync
                //if (parentChildSync) {
                //    switch (columnName) {
                //        case "ADMIN_CONTENT_TYPE" :
                //        case "IS_RESTRICTED" :
                //        case "MANAGING_UNIT" :
                //       case "MAX_IDS_SIZE" :
                //        case "INTERNAL_IDS_SIZE" :
                //        case "PUBLIC_USE" :                  
                //        case "SEC_POLICY_ID" :
                //        case "SI_DEL_RESTS" :
                //            continue;
                //    }
                // }
    
    public Update(String uoiId, String tableName) {
        super(uoiId, tableName);

    }
    
    public void populateSql(String columnName, String dataValue) {
        sql = "UPDATE towner." + tableName + " SET " + columnName + "= '" + dataValue + "'";
    }
    
    public void appendToExistingSql(String columnName, String dataValue) {
        sql =  getSql() + ", " + columnName + "= '" + dataValue + "'";
    }
    
}
