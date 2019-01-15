/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlData;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 * This class holds the SQL command and associated information retrieved from the SQL-XML configuration file
 */
public class XmlCisSqlCommand {
   
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private String appendDelimiter;
    private String dbName;
    private String sqlQuery;
    private String tableName;
    private String operationType;
    private String delClause;
    
    public XmlCisSqlCommand() {
    
    }
    
    public String getAppendDelimiter() {
        return this.appendDelimiter;
    }
    
    public String getDelClause() {
        return this.delClause;
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
    
    public void setDelClause(String delClause) {
        this.delClause = delClause;
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
    
    public boolean setValuesFromXml(XmlData xmlInfo) {
        setSqlQuery(xmlInfo.getDataValue());
        setAppendDelimeter(xmlInfo.getAttributeData("multiResultDelim"));
        setTableName(xmlInfo.getAttributeData("destTableName"));
        setOperationType(xmlInfo.getAttributeData("operationType"));
        setDelClause(xmlInfo.getAttributeData("delClause"));
        
        if (xmlInfo.getAttributeData("dbConn") == null) {
            logger.log(Level.SEVERE, "Error: Unable to determine database to update");
            return false;
        }
        setDbName(xmlInfo.getAttributeData("dbConn"));
        
        return true;
    }

}
