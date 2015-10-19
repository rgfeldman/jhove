/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.si.CDIS.DAMS.Database;

/**
 *
 * @author rfeldman
 */
public class SiPreservationMetadata {
 
    String assetSourceDate;
    String preservationIdNumber;
    String uoiid;
    
    public String getAssetSourceDate() {
        return this.assetSourceDate;
    }
    
    public String getUoiid () {
        return this.uoiid;
    } 
    
    public String getPreservationIdNumber () {
        return this.preservationIdNumber;
    }
    
    public void setAssetSourceDate(String assetSourceDate) {
        this.assetSourceDate = assetSourceDate;
    }
    
    public void setPreservationIdNumber(String preservationIdNumber) {
        this.preservationIdNumber = preservationIdNumber;
    }
    
    public void setUoiid (String uoiid) {
        this.uoiid = uoiid;
    }
    
    public void insertRow () {
        String sql = "Insert into si_preservation_metadata (" +
                        "uoi_id, " +
                        "preservation_id_number, " +
                        "preservation_id_type, " +
                        "asset_source, " +
                        "asset_source_date )" +
                      "values (" +
                        "'" + getUoiid() +"', " +
                        "'" + getPreservationIdNumber() +"', " +
                        "'md5', " +
                        "NEED VENDOR NAME" +
                        "NEED DATE OF FILE";
                        
                        
    }
    
    /*PRESERVATION_ID_TYPE: MD5
ASSET_SOURCE: Vendor name
ASSET_SOURCE_DATE: 
            */
}
