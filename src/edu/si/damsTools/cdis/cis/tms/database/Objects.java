/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.damsTools.cdis.cis.tms.database;

import edu.si.damsTools.DamsTools;
import edu.si.damsTools.cdisutilities.Transform;
import edu.si.damsTools.utilities.XmlUtils;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
        
public class Objects {
    
    private final static Logger logger = Logger.getLogger(DamsTools.class.getName());
    
    private int objectID;
    private String objectNumber;
    
    
    public int getObjectId () {
        return this.objectID;
    }
    
    public String getObjectNumber () {
        return this.objectNumber;
    }
    
   
    public void setObjectId (int objectID) {
        this.objectID = objectID;
    }
    
    private void setObjectNumber (String objectNumber) {
        this.objectNumber = objectNumber;
    }
        

    /*  Method :        mapFileNameToBarcode
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS Barcode
        RFeldman 2/2015
    */
    public boolean mapFileNameToBarcode (String barcode) {
      
        //Strip all characters in the barcode after the underscore to look up the label
        if (barcode.contains("_")) {
           barcode = barcode.substring(0,barcode.indexOf('_')); 
        }
        
        String sql = "Select ObjectID " +
              "from BCLabels bcl, " +
              "ObjComponents obc " +
              "where bcl.id = obc.Componentid " +
              "and bcl.TableID = 94 " +
              "and bcl.LabelUUID = '" + barcode + "' " +
              "and ObjectID > 0";
        
        logger.log(Level.FINEST, "SQL: {0}", sql); 
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                setObjectId (rs.getInt(1));
            }        
            else {
                logger.log(Level.FINEST, "Unable to find Object from Barcode:{0}", barcode);
                return false;
            }
	}
            
	catch(Exception e) {
            e.printStackTrace();
            return false;
	}
        
        return true;
    }
    
    /*  Method :        mapFileNameToObjectID
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS ObjectID
        RFeldman 2/2015
    */
    public boolean mapFileNameToObjectID(String extensionlessFileName) {
        
        Integer objID;
        
        try {
            
            objID = Integer.parseInt(extensionlessFileName.substring(0, extensionlessFileName.indexOf('_')));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to find ObjectID as part of damsFileName", extensionlessFileName);
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
                    setObjectId (objID);
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

    
    /*  Method :        mapFileNameToObjectNumber
        Arguments:      
        Returns:      
        Description:    Finds the objectID and populates object member variables based on the DAMS image filename to the TMS ObjectNumber
        RFeldman 2/2015
    */
    public boolean mapFileNameToObjectNumber (String damsImageFileName){

        String damsDelimiter;
        String tmsDelimiter;
        int imageObjectTrunc;
        
        try {
            // populate ObjectNumber using various formats specified in the config file
            damsDelimiter = XmlUtils.getConfigValue("damsDelimiter");
            tmsDelimiter = XmlUtils.getConfigValue("tmsDelimiter");
            imageObjectTrunc = Integer.parseInt(XmlUtils.getConfigValue("imageObjectTrunc"));
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }
        
        Transform transform = new Transform();
        
        logger.log(Level.FINER,"Need to find Object for filename...before reformat " + damsImageFileName);
        
        //Call Transform to change delimter...as well as the truncation rule for finding the object based on the DAMS image fileName
        setObjectNumber(transform.transform(damsImageFileName,damsDelimiter,tmsDelimiter,0,imageObjectTrunc));
        
        
        logger.log(Level.FINER,"NEW TMS ObjectNumber: " + getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        boolean objectNumPopulated = populateIdForObjectNumber();
        return objectNumPopulated;
        
    }
    
    public boolean mapAltColumnToObject(String uoiId) {
        
        String[] tableAndColumn = XmlUtils.getConfigValue("altObjTabCol").split(",");
        
        String table = tableAndColumn[0];
        String column = tableAndColumn[1];
        
        String obNumsql =    "SELECT " + column +
                        " FROM " + table +
                        " WHERE uoi_id = '" + uoiId + "'";
        
        logger.log(Level.FINER,"sql: " + obNumsql);
        
         try (PreparedStatement pStmt = DamsTools.getDamsConn().prepareStatement(obNumsql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            if (rs.next()) {
                    setObjectNumber (rs.getString(1));
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
         
        logger.log(Level.FINER,"TMS ObjectNumber: " + getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        boolean objectNumPopulated = populateIdForObjectNumber();
        return objectNumPopulated;
    }
    
    
    /*  Method :        populateObjectIDByRenditionId
        Arguments:      
        Returns:      
        Description:    Finds, and sets the objectID based on the RenditionID 
        RFeldman 2/2015
    */
    public boolean populateMinObjectIDByRenditionId (Integer renditionId) {
        
        String sql =    "select min(a.ObjectID) " +
                        "from Objects a, " +
                        "MediaXrefs b, " +
                        "MediaMaster c, " +
                        "MediaRenditions d " +
                        "where a.ObjectID = b.ID " +
                        "and b.MediaMasterID = c.MediaMasterID " +
                        "and b.TableID = 108 " +
                        "and c.MediaMasterID = d.MediaMasterID " +
                        "and d.RenditionID = " + renditionId;
        
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
   
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getInt(1) + " For RenditionID: " + renditionId);
                setObjectId(rs.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public boolean populateObjectNumberForObjectID() {
        String sql = "SELECT ObjectNumber " +
                    "FROM objects " +
                    "WHERE objectID = " + getObjectId();
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setObjectNumber(rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain ObjectNumber for objectID", e );
                return false;
        }
        return true;
    }
    
    public boolean populateIdForObjectNumber() {
        String sql = "SELECT ObjectId " +
                    "FROM objects " +
                    "WHERE objectNumber = '" + getObjectNumber() + "'";
        
        logger.log(Level.FINEST,"SQL! " + sql);
        try (PreparedStatement pStmt = DamsTools.getCisConn().prepareStatement(sql);
                ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs != null && rs.next()) {
                setObjectId(rs.getInt(1));
                return true;
            }  
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain ObjectNumber for objectID", e );
        }
        return false;
    }
}

