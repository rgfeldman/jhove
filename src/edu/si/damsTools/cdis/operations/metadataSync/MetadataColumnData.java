/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.operations.metadataSync;

import edu.si.damsTools.DamsTools;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rfeldman
 */
public class MetadataColumnData {
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private final String columnName;
    private final String columnValue;
    
    public String getColumnName() {
        return this.columnName;
    }
    
    public String getColumnValue() {
        return this.columnValue;
    }
    
    public MetadataColumnData(String inColumnName, String inColumnValue) {
        columnName = inColumnName;
        columnValue = inColumnValue;
        
        logger.log(Level.FINEST, "COL: " +  columnName );
        logger.log(Level.FINEST, "VAL: " +  columnValue );
        
    }
    
}
