/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

import edu.si.CDIS.CDIS;
import java.sql.SQLException;
import java.util.logging.Level;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Logger;


public class SiAssetMetaData {
    //class attributes
    String adminContentType;
    String alternateIdentifier1;
    String caption;
    String credit;
    String description;
    String digitalItemNotes;
    String groupTitle;
    String intellectualContentCreator;
    String isRestricted;
    String keywords;
    String maxIdsSize;
    String namedPerson;
    String otherConstraints;
    String owningUnitUniqueName;
    String primaryCreator;
    String rightsHolder;
    String seriesTitle;
    String termsAndRestrictions;
    String title;
    String uoiid;  
    String useRestrictions;
    String sourceSystemId;
    String workCreationDate;
    String notes;

	Connection damsConn;
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
      
    
    // get functions

    public String getAdminContentType () {
        return this.adminContentType;
    }
        
    public String getAlternateIdentifier1 () {
        return this.alternateIdentifier1;
    }
    
    public String getCaption () {
        return this.caption;
    }
    
    public String getCredit () {
        return this.credit;
    }
    
    public String getDescription () {
        return this.description;
    }
    
    public String getDigitalItemNotes () {
        return this.digitalItemNotes;
    }
    
    public String getGroupTitle () {
        return this.groupTitle;
    } 
    
    public String getIsRestricted () {
        return this.isRestricted;
    } 
     
    public String getIntellectualContentCreator () {
        return this.intellectualContentCreator;
    } 
    
    public String getKeywords () {
        return this.keywords;
    } 
    
    public String getMaxIdsSize () {
        return this.maxIdsSize;
    } 
    
    public String getNamedPerson () {
        return this.namedPerson;
    }
    
    public String getNotes() {
		return notes;
    }
    
    public String getOtherConstraints () {
        return this.otherConstraints;
    }
    
    public String getOwningUnitUniqueName () {
        return this.owningUnitUniqueName;
    }
    
    public String getPrimaryCreator () {
        return primaryCreator;
    } 
    
    public String getRightsHolder () {
        return rightsHolder;
    } 
    
    public String getSeriesTitle () {
        return seriesTitle;
    } 
    
    public String getTitle () {
        return title;
    } 
    
    public String getUoiid () {
        return uoiid;
    } 
    
    public String getTermsAndRestrictions () {
        return termsAndRestrictions;
    } 
    
    public String getUseRestrictions () {
        return useRestrictions;
    } 

    public String getSourceSystemId () {
        return sourceSystemId;
    } 
    
    public String getWorkCreationDate () {
        return workCreationDate;
    } 
    
    
    
    // set functions    
    public void setAdminContentType(String adminContentType) {
        adminContentType = adminContentType;
        this.adminContentType = adminContentType;
    }
    
    public void setAlternateIdentifier1(String alternateIdentifier1) {
        
        if ( alternateIdentifier1.length() > 40 ) {
        	this.alternateIdentifier1 = alternateIdentifier1.substring(0,39);
        }
        else {
            this.alternateIdentifier1 = alternateIdentifier1;
        }
    }
    
    public void setCaption(String caption) {
        
        if ( caption.length() > 4000 ) {
        	this.caption = caption.substring(0,3999);
        }
        else {
            this.caption = caption;
        }
    }
    
    public void setCaption(String caption, String delimiter) {
        
        if ( this.caption != null ) {
            
            if (this.caption.length() + caption.length() > 4000 ) {
                this.caption = this.caption += delimiter + caption.substring(0,3999);
            }
            else {
                this.caption = this.caption += delimiter + caption;
            }
        }
        else {
                this.caption = caption;
        }
    }
    
    public void setCredit(String credit) {
        
        if ( credit.length() > 2000 ) {
        	this.credit = credit.substring(0,1999);
        }
        else {
            this.credit = credit;
        }
    }
    
    public void setDescription(String description) {
         
        if ( description.length() > 4000 ) {
        	this.description = description.substring(0,3999);
        }
        else {
            this.description = description;
        }
    }
    
    public void setDigitalItemNotes(String digitalItemNotes) {
        
        if ( digitalItemNotes.length() > 200 ) {
        	this.digitalItemNotes = digitalItemNotes.substring(0,199);
        }
        else {
            this.digitalItemNotes = digitalItemNotes;
        }
    }
    
    public void setGroupTitle(String groupTitle) {
        
        if (groupTitle.length() > 2000 ) {
            this.groupTitle = groupTitle.substring(0,1999);
        }
        else {
            this.groupTitle = groupTitle;
        }
    }
    
    public void setIntellectualContentCreator(String intellectualContentCreator) {
                
        if (intellectualContentCreator.length() > 999 ) {
            this.intellectualContentCreator = intellectualContentCreator.substring(0,1999);
        }
        else {
            this.intellectualContentCreator = intellectualContentCreator;
        }
    }
    
    public void setIsRestricted(String isRestricted) {   
       
        if (isRestricted.equals("Yes")) {
            this.isRestricted = "Yes";
        }
        else {
            this.isRestricted = "No";
        }
    }
    
    public void setKeywords(String keywords) {
        
        if (keywords.length() > 4000 ) {
            this.keywords = keywords.substring(0,3999);
        }
        else {
            this.keywords = keywords;
        }
    }
    
    public void setKeywords(String keywords, String delimiter) {
        
        if ( this.keywords != null ) {
            
            if (this.keywords.length() + keywords.length() > 4000 ) {
                this.keywords = this.keywords += delimiter + keywords.substring(0,3999);
            }
            else {
                this.keywords = this.keywords += delimiter + keywords;
            }
        }
        else {
                this.keywords = keywords;   
        }
    }
    
    public void setMaxIdsSize(String maxIdsSize) {
        
        //make sure our maxIDS Size is an Int
        try {
            Integer maxIdsSizeInt = Integer.parseInt(maxIdsSize);
            
            this.maxIdsSize = maxIdsSizeInt.toString();
            
        } catch (Exception e) {
            // We didnt have a valid (numeric) max ids size, set it to the default (null)
            //dont set maxIds size to anything
        }
        
    }
    
    public void setNamedPerson(String namedPerson) {
 
	if (namedPerson.length() > 2000) {
                this.namedPerson = namedPerson.substring(0,1999);
        }
        else {
            this.namedPerson = namedPerson;
        }
    }
    
    public void setNamedPerson (String namedPerson, String delimiter) {
        
        if ( this.namedPerson != null ) {
            
            if (this.namedPerson.length() + namedPerson.length() > 2000 ) {
                this.namedPerson = this.namedPerson += delimiter + namedPerson.substring(0,1999);
            }
            else {
                this.namedPerson = this.namedPerson += delimiter + namedPerson;
            }
        }
        else {
                this.namedPerson = namedPerson;   
        }
    }
    
    public void setNotes(String notes) {

        if (notes.length() > 4000) {
                this.notes = notes.substring(0,3999);
        }
        else {
            this.notes = notes;
        }
    }            
    
    public void setOtherConstraints(String otherConstraints) {
        
	if (otherConstraints.length() > 2000) {
                this.otherConstraints = otherConstraints.substring(0,1999);
        }
        else {
            this.otherConstraints = otherConstraints;
        }
    }
    
    public void setOwningUnitUniqueName(String owningUnitUniqueName) {
        this.owningUnitUniqueName = owningUnitUniqueName;
    }
    
    public void setPrimaryCreator(String primaryCreator) {
        
	if (primaryCreator.length() > 1000) {
                this.primaryCreator = primaryCreator.substring(0,999);
        }
        else {
            this.primaryCreator = primaryCreator;
        }
    }
    
    public void setRightsHolder(String rightsHolder) {
        
	if (rightsHolder.length() > 2000) {
                this.rightsHolder = rightsHolder.substring(0,1999);
        }
        else {
            this.rightsHolder = rightsHolder;
        }
    }
    
    public void setSeriesTitle(String seriesTitle) {
        
	if (seriesTitle.length() > 150) {
                this.seriesTitle = seriesTitle.substring(0,149);
        }
        else {
            this.seriesTitle = seriesTitle;
        }
    }
    
    public void setTermsAndRestrictions(String termsAndRestrictions) {
        
	if (termsAndRestrictions.length() > 4000) {
                this.termsAndRestrictions = termsAndRestrictions.substring(0,3999);
        }
        else {
            this.termsAndRestrictions = termsAndRestrictions;
        }
    }
    
    public void setTitle(String title) {
        
	if (title.length() > 2000) {
                this.title = title.substring(0,1999);
        }
        else {
            this.title = title;
        }
    }
    
    public void setUoiid(String uoiid) {
           this.uoiid = uoiid;
    }
    
    public void setUseRestrictions(String useRestrictions) {
        
        if (useRestrictions.length() > 250 ) {
            this.useRestrictions = useRestrictions.substring(0,249);
        }
        else {
            this.useRestrictions = useRestrictions;
        }
    }

    public void setSourceSystemId(String sourceSystemId) {
	this.sourceSystemId = sourceSystemId;
    }
    
    public void setWorkCreationDate(String workCreationDate) {
        
        // get rid of extra leading 0's in the date
        workCreationDate = workCreationDate.replaceAll(" 0", "");
        
        if (workCreationDate.length() > 255 ) {
            this.workCreationDate = workCreationDate.substring(0,254);
        }
        else {
            this.workCreationDate = workCreationDate;
        }
    }
    
    /*  Method :        updateDAMSSourceSystemID
        Arguments:      
        Description:    
        RFeldman 2/2015
    */
    
    public int updateDAMSSourceSystemID (Connection damsConn, String uoiid, String sourceSystemId) {
        int recordsUpdated = 0;
        PreparedStatement pStmt = null;
        
        String sql = "update SI_ASSET_METADATA set source_system_id = '" + sourceSystemId + "' " +
                    "where UOI_ID = '" + uoiid + "'";

        logger.log(Level.FINEST, "SQL! {0}", sql);
        
        try {
            
            pStmt = damsConn.prepareStatement(sql);
            recordsUpdated = pStmt.executeUpdate(sql);
            
        
            logger.log(Level.FINEST,"Rows Updated in DAMS! {0}", recordsUpdated);
            
        } catch (Exception e) {
                e.printStackTrace();
        }finally {
                try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
            
        return recordsUpdated;
        
    }
    
    public boolean populateOwningUnitUniqueName (Connection damsConn) {
        
        PreparedStatement pStmt = null;
        ResultSet rs = null;
  
        String sql = "SELECT owning_unit_unique_name FROM si_asset_metadata " +
                    "WHERE uoi_id = '" + getUoiid() + "'";
        
        try {
            logger.log(Level.FINEST,"SQL! " + sql); 
             
            pStmt = damsConn.prepareStatement(sql);
            rs = pStmt.executeQuery();
            
            if (rs != null && rs.next()) {
                setOwningUnitUniqueName (rs.getString(1));
            }   
            
        } catch (Exception e) {
                logger.log(Level.FINER, "Error: unable to obtain FileName from cdis_map", e );
                return false;
        
        }finally {
            try { if (pStmt != null) pStmt.close(); } catch (SQLException se) { se.printStackTrace(); }
            try { if (rs != null) rs.close(); } catch (SQLException se) { se.printStackTrace(); }
        }
        return true;
    }
}
