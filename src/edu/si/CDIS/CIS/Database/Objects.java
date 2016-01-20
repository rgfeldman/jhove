/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.CIS.Database;

import edu.si.CDIS.CDIS;
import edu.si.CDIS.utilties.Transform;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;
        
public class Objects {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    
    private int objectID;
    private String objectNumber;
    
    
    public int getObjectID () {
        return this.objectID;
    }
    
    public String getObjectNumber () {
        return this.objectNumber;
    }
    
   
    public void setObjectID (int objectID) {
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
           barcode = barcode.substring(0,barcode.indexOf("_")); 
        }
        
        String sql = "Select ObjectID " +
              "from BCLabels bcl, " +
              "ObjComponents obc " +
              "where bcl.id = obc.Componentid " +
              "and bcl.TableID = 94 " +
              "and bcl.LabelUUID = '" + barcode + "'";
        
        logger.log(Level.FINEST, "SQL: {0}", sql); 
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {

            if (rs.next()) {
                setObjectID (rs.getInt(1));
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
            
            objID = Integer.parseInt(extensionlessFileName.substring(0, extensionlessFileName.indexOf("_")));
            
        } catch(Exception e) {
            logger.log(Level.FINEST, "Unable to find ObjectID as part of damsFileName", extensionlessFileName);
            return false;
	}

        //Confirm that the objectID exists before we set it in the object
        String sql = "select 'X' " +
                    "from Objects " +
                    "where ObjectID = " +  objID;
        
        logger.log(Level.FINEST, "SQL: {0}", sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() ) {
           
            if (rs.next()) {
                    setObjectID (objID);
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
        String locateByLetterRange;
        int imageObjectTrunc;
        
        try {
            // populate ObjectNumber using various formats specified in the config file
            damsDelimiter = CDIS.getProperty("damsDelimiter");
            tmsDelimiter = CDIS.getProperty("tmsDelimiter");
            locateByLetterRange = CDIS.getProperty("locateByLetterRange");
            imageObjectTrunc = Integer.parseInt(CDIS.getProperty("imageObjectTrunc"));
        } catch (Exception e) {
                e.printStackTrace();
                return false;
        }
            
        String tmpObjectNumber = null;
        
        Transform transform = new Transform();
        
        logger.log(Level.FINER,"Need to find Object for filename...before reformat " + damsImageFileName);
        
        //Find the objectNumber based on the Rendition number
        if (damsDelimiter.equals("ACM")) {
            //Drop acmobj- from the front
            tmpObjectNumber = damsImageFileName.replaceAll("acmobj-", "");  
          
            //To get the object we also truncate everything after a dash
            if (tmpObjectNumber.contains("-")) {
                tmpObjectNumber = tmpObjectNumber.substring(0, tmpObjectNumber.indexOf("-"));
            }
            
            if (tmpObjectNumber.length() > 8) {
                //Set Dots every four
                setObjectNumber(tmpObjectNumber.substring(0, 4) + "." + tmpObjectNumber.substring(4, 8) + "." +  tmpObjectNumber.substring(8));   
            }
            else {
                setObjectNumber(tmpObjectNumber);
            }
        }
        else {
            //Call Transform to change delimter...as well as the truncation rule for finding the object based on the DAMS image fileName
            setObjectNumber(transform.transform(damsImageFileName,damsDelimiter,tmsDelimiter,0,imageObjectTrunc));
        }
        
        logger.log(Level.FINER,"NEW TMS ObjectNumber: " + getObjectNumber());
    
        // Obtain the ObjectID based on the ObjectName that was determined above. 
        String sql =    "select ObjectID " +
                        "from Objects " +
                        "where ObjectNumber = '" + getObjectNumber() + "'";
                    
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
            ResultSet rs = pStmt.executeQuery() ) {
            
            if (rs.next()) {
                setObjectID(rs.getInt(1));
            }
            else {
                if (locateByLetterRange.equals("true")) {
                    locateObjectIDLetterComponent();
                }
                if (getObjectID() == 0) {
                    return false;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        
        return true;
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
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql);
             ResultSet rs = pStmt.executeQuery() ) {
   
            if (rs.next()) {
                logger.log(Level.FINER,"Located ObjectID: " + rs.getInt(1) + " For RenditionID: " + renditionId);
                setObjectID(rs.getInt(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
     /*  Method :        locateObjectIDLetterComponent
        Arguments:      
        Returns:      
        Description:    Finds the objectID when the normal method fails,
                        ObjectID will be looked up by dropping final letter from the expected ObjectNumber 
                        and finding the object based on a letter range
                        (ex 2013_201_3c_001 -> 2013_201_3a-c OR 2013_201_3ac )
        RFeldman 2/2015
    */
    private void locateObjectIDLetterComponent() {

        char lastChar;
        logger.log(Level.FINEST,"Attempting to locate object by letter component");
        
        // only continue if the ObjectNumber we were expecting ends in a letter.
        if (! getObjectNumber().isEmpty() ) {          
            lastChar = getObjectNumber().charAt(getObjectNumber().length() -1);
            
            if (! Character.isLetter(lastChar)) {
                // The last character is not a letter....
                logger.log(Level.FINEST,"Last character not a letter, returning");
                return;
            }
        }      
        
        //Remove the last character from the objectNumber...and add an 'a'
        setObjectNumber(getObjectNumber().substring(0, getObjectNumber().length()-1) + "a");
    
        //look for the object number with like 
        String sql =    "select ObjectID " +
                        "from Objects " +
                        "where ObjectNumber like '" + getObjectNumber() + "[b-z]' " +
                        "union " +
                        "select ObjectID " +
                        "from Objects " +
                        "where ObjectNumber like '" + getObjectNumber() + "-[b-z]' ";
                    
        logger.log(Level.FINEST,"SQL! " + sql);
        
        try (PreparedStatement pStmt = CDIS.getCisConn().prepareStatement(sql); 
             ResultSet rs = pStmt.executeQuery() )  {

            if (rs.next()) {
                setObjectID(rs.getInt(1));
            }            
        } catch (Exception e) {
            e.printStackTrace();
        } 
    } 
}

