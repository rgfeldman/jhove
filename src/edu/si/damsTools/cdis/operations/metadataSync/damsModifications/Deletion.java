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
public class Deletion extends ModificationsForDams {
    
    public Deletion(String uoiId, String tableName) {
        super(uoiId, tableName);
    }
    
    public void populateSql() {
        sql = "DELETE FROM towner." + tableName + " WHERE UOI_ID = '" + uoiId + "'";
    }
    
}
