/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.modules;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import edu.si.damsTools.cdis.dams.DamsRecord;
import edu.si.damsTools.cdisutilities.Transform;
import edu.si.damsTools.utilities.StringUtils;
import edu.si.damsTools.cdis.cis.tms.database.Objects;


/**
 *
 * @author rfeldman
 */
public class Object implements ModuleType{
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private Objects objects;  // Refers to 'Objects' table in Database
    private String mappedMethod;
    
    public Object () {
        
    }
    
    public String returnMappedMethod (){
        return this.mappedMethod;
    }
    
    public Integer returnRecordId  () {
        return this.objects.getObjectId();
    }
    
    public String returnTableId() {
        return "108";
    }
    
    public boolean populateRecordId(DamsRecord damsRecord) {
        objects = new Objects();
        //There are several ways to obtain the objectID, we go through each one in the config file until we get an objectId match.
        
        if (! XmlUtils.getConfigValue("assignCisRecordId").equals("false")) {
            objects.setObjectId(Integer.parseInt(XmlUtils.getConfigValue("assignCisRecordId")));
            logger.log(Level.FINER, "Set object to ObjectID, mapMethod = assigned");
            mappedMethod = "assigned";
            return true;
        }
                
        if (XmlUtils.getConfigValue("mapFileNameToCisBarcode").equals("true")) {
            mapFileNameToBarcode(damsRecord.getUois().getName());
            if (objects.getObjectId() != null) {
                logger.log(Level.FINER, "Set object to ObjectID, mapMethod = barcode");
                mappedMethod = "barcode";
                return true;
            }
        }
        
        if (XmlUtils.getConfigValue("mapFileNameToCisRecordName").equals("true")) {
            mapFileNameToObjectNumber(damsRecord.getUois().getName());
            if (objects.getObjectId() != null) {
                logger.log(Level.FINER, "Set object to ObjectID, mapMethod = filenameToRecordName");
                mappedMethod = "filenameToRecordName";
                return true;
            }
        }
         
        if (XmlUtils.getConfigValue("mapFileNameToCisRecordId").equals("true")) {              
            mapFileNameToId(damsRecord.getUois().getName());
            if (objects.getObjectId() != null) {
                logger.log(Level.FINER, "Set object to ObjectID, mapMethod = filenameToRecordId");
                mappedMethod = "filenameToRecordId";
                return true;
            }
        }
       
        if (XmlUtils.getConfigValue("mapDamsDataToCisRecordName").equals("true")) {
            mapAltColumnToObject(damsRecord.getUois().getUoiid());
            if (objects.getObjectId() != null) {
                logger.log(Level.FINER, "Set object to ObjectID, mapMethod = damsDataToRecordName");
                mappedMethod = "damsDataToRecordName";
                return true;
            }
        }

        return false;
        
    }
    
    
    public boolean mapAltColumnToObject(String uoiId) {
        
        String[] tableAndColumn = XmlUtils.getConfigValue("damsTablColToMap").split(",");
        
        String table = tableAndColumn[0];
        String column = tableAndColumn[1];
        
        String obNumsql =    "SELECT " + column +
                        " FROM " + table +
                        " WHERE uoi_id = '" + uoiId + "'";
        
        logger.log(Level.FINER,"sql: " + obNumsql);
        
         try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(obNumsql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            if (rs.next()) {
                    objects.setObjectNumber(rs.getString(1));
                }        
            
            else {
                logger.log(Level.FINEST, "Unable to find Object from table:", obNumsql);
                return false;
            }
        }
	catch(Exception e) {
            e.printStackTrace();
            return false;
	}     
         
        logger.log(Level.FINER,"TMS ObjectNumber: " + objects.getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        boolean objectNumPopulated = objects.populateIdForObjectNumber();
        return objectNumPopulated;
    }
    
    
    /*  Method :        mapFileNameToBarcode
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS Barcode
        RFeldman 2/2015
    */
    public boolean mapFileNameToBarcode (String damsFileName) {
      
        String barcode = StringUtils.getExtensionlessFileName(damsFileName);
        
        //Strip all characters in the barcode after the underscore to look up the label
        if (barcode.contains("_")) {
           barcode = barcode.substring(0,barcode.indexOf('_')); 
        }
        
        String sql = "SELECT ObjectID " +
              "FROM BCLabels bcl " +
              "INNER JOIN ObjComponents obc " +
              "ON bcl.id = obc.Componentid " +
              "WHERE bcl.TableID = 94 " +
              "AND bcl.LabelUUID = '" + barcode + "' " +
              "AND ObjectID > 0";
        
        logger.log(Level.FINEST, "SQL: {0}", sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                objects.setObjectId(rs.getInt(1));
            }        
            else {
                logger.log(Level.FINEST, "Unable to find Object from Barcode:{0}", barcode);
                return false;
            }
	}
            
	catch(Exception e) {
            logger.log(Level.FINEST, "Error, Unable to find Object from Barcode:{0}", barcode);
            return false;
	}
        
        return true;
    }
    
    /*  Method :        mapFileNameToObjectNumber
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS ObjectNumber
        RFeldman 2/2015
    */
    public boolean mapFileNameToObjectNumber (String damsFileName){

        String damsDelimiter;
        String tmsDelimiter;
        int imageObjectTrunc;
        
        try {
            // populate ObjectNumber using various formats specified in the config file
            damsDelimiter = XmlUtils.getConfigValue("damsDelimiter");
            tmsDelimiter = XmlUtils.getConfigValue("tmsDelimiter");
            imageObjectTrunc = Integer.parseInt(XmlUtils.getConfigValue("damsToCisTrunc"));
            
        } catch (Exception e) {
            logger.log(Level.FINER,"Error obtaining Object from CIS ");
            return false;
        }
        
        Transform transform = new Transform();
        logger.log(Level.FINER,"Need to find Object for filename...before reformat " + StringUtils.getExtensionlessFileName(damsFileName));
               
        //Call Transform to change delimter...as well as the truncation rule for finding the object based on the DAMS image fileName
        objects.setObjectNumber(transform.transform(StringUtils.getExtensionlessFileName(damsFileName),damsDelimiter,tmsDelimiter,0,imageObjectTrunc));
        logger.log(Level.FINER,"NEW TMS ObjectNumber: " + objects.getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        boolean objectNumPopulated = objects.populateIdForObjectNumber();
        return objectNumPopulated;
        
    }
    
     /*  Method :        mapFileNameToId
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS ObjectID
        RFeldman 2/2015
    */
    public boolean mapFileNameToId(String fileName) {
        
        Integer objID;     
        try {
            
            if (fileName.contains("_")) {
                objID = Integer.parseInt(fileName.substring(0, fileName.indexOf('_')));
            }
            else objID = Integer.parseInt(StringUtils.getExtensionlessFileName(fileName));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to find ObjectID as part of damsFileName", fileName);
            return false;
	}

        //Confirm that the objectID exists before we set it in the object
        String sql = "select 'X' " +
                    "from Objects " +
                    "where ObjectID = " +  objID;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            if (rs.next()) {
                    objects.setObjectId(objID);
                }        
            else {
                logger.log(Level.FINEST, "Unable to find Object from ObjectID:{0}", objID);
                return false;
            }
        }
	catch(Exception e) {
            e.printStackTrace();
            return false;
	}     
        return true;
    }
    
    
}
