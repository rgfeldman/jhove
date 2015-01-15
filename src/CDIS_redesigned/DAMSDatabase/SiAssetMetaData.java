/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS_redesigned.DAMSDatabase;

/**
 *
 * @author rfeldman
 */
public class SiAssetMetaData {
    //class attributes
    String credit;
    String groupTitle;
    String isRestricted;
    String keywords;
    String maxIdsSize;
    String title;
    String useRestrictions;
    String sourceSystemId;
    String workCreationDate;
        
    
    // get functions
    public String getCredit () {
             return credit;
    }
    
    public String getGroupTitle () {
        return groupTitle;
    } 
    
    public String getIsRestricted () {
        return isRestricted;
    } 
    
    public String getKeywords () {
        return keywords;
    } 
    
    public String getMaxIdsSize () {
        return maxIdsSize;
    } 
    
    public String getTitle () {
        return title;
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
    public void setCredit(String credit) {
        if ( credit.length() > 2000 ) {
        	this.credit = credit.substring(0,1999);
        }
        else {
            this.credit = credit;
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
    
    public boolean setIsRestricted(String isRestricted) {
        if (isRestricted == "Yes" || isRestricted == "No") {
            this.isRestricted = isRestricted;
            return true;
        }
        else {
            return false;
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
    
    public void setMaxIdsSize(String maxIdsSize) {
	this.maxIdsSize = maxIdsSize.trim();
    }
    
    public void setTitle(String title) {
	if (title.length() > 2000) {
                this.title = title.substring(0,1999);
        }
        else {
            this.title = title;
        }
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
        if (workCreationDate.length() > 255 ) {
            this.workCreationDate = workCreationDate.substring(0,254);
        }
        else {
            this.workCreationDate = workCreationDate;
        }
    }
        
}
