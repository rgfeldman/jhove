/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.dams.database;

import java.util.HashMap;

/**
 *
 * @author rfeldman
 */
public class MetadataTransaction {
   
    private String appendDelimiter;
    private String dbName;
    private String sqlQuery;
    private String tableName;
    private String operationType;
    
    
    public MetadataTransaction() {

    }
    
    public String getAppendDelimiter() {
        return this.appendDelimiter;
    }
    
    public String getDbName() {
        return this.dbName;
    }
    
    public String getOperationType() {
        return this.operationType;
    }
        
    public String getSqlQuery () {
        return this.sqlQuery;
    }
        
    public String getTableName() {
        return this.tableName;
    }
   
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    
    public void setAppendDelimeter(String appendDelimiter) {
        this.appendDelimiter = appendDelimiter;
    }
      
    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }
    
    public void setSqlQuery(String sqlQuery) {
        this.sqlQuery = sqlQuery;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
              
    
}
