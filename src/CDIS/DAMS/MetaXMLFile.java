/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CDIS.DAMS;

import com.jamesmurty.utils.XMLBuilder;
import java.io.File;
import CDIS.CDIS;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Properties;
import java.io.FileWriter;
import java.sql.Connection;

public class MetaXMLFile {
    
    private final static Logger logger = Logger.getLogger(CDIS.class.getName());
    XMLBuilder xml;
    
    public void contentCreate () {
    
        try {
            xml = XMLBuilder.create("tmsAsset");
            
            xml.e("x:xmpmeta")
		.a("xmlns:x", "adobe:ns:meta/")
		.a("x:xmptk", "XMP Core 5.1.2")
                .e("rdf:RDF")
                    .a("xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#")
                    .e("rdf:Description")
                        .a("rdf:about", "")
                        .a("xmlns:photoshop", "http://ns.adobe.com/photoshop/1.0")
                        .a("xmlns:xmpRights", "http://ns.adobe.com/xap/1.0/rights")
                        .a("xmlns:dc", "http://purl.org/dc/elements/1.1")
                        .a("xmlns:Iptc4xmpCore", "http://iptc.org/std/Iptc4xmpCore/1.0/xmlns")
                        .a("xmlns:Iptc4xmpExt", "http://iptc.org/std/Iptc4xmpExt/2008-02-29/")
                        .a("xmlns:plus", "http://ns.useplus.org/ldf/xmp/1.0/")
                        .a("xmlns:aux", "http://ns.adobe.com/exif/1.0/aux/")
                        .a("xmlns:photomechanic", "http://ns.camerabits.com/photomechanic/1.0/")
                        .a("photoshop:City", "City")
                        .a("photoshop:State", "State")
                        .a("photoshop:Country", "Country")
			.a("photoshop:AuthorsPosition", "Creator Job Title")
			.a("photoshop:Credit", "Credit Line")
			.a("photoshop:Source", "Source")
			.a("photoshop:CaptionWriter", "Caption Writer")
			.a("photoshop:Headline", "Headline")
			.a("photoshop:Instructions", "Instructions")
			.a("photoshop:TransmissionReference", "Job Identifier")
			.a("photoshop:DateCreated", "2001-01-01T00:00:00-04:00")
			.a("xmpRights:Marked", "True")
			.a("Iptc4xmpCore:CountryCode", "ISO Country")
			.a("Iptc4xmpCore:Location", "Sublocation")
			.a("Iptc4xmpCore:IntellectualGenre", "Intellectual Genre")
			.a("Iptc4xmpExt:AddlModelInfo", "Additional Model info")
			.a("Iptc4xmpExt:MaxAvailWidth", "Max Available Width")
			.a("Iptc4xmpExt:MaxAvailHeight", "Max Available Height")
			.a("Iptc4xmpExt:DigitalSourceType", "http://cv.iptc.org/newscodes/digitalsourcetype/digitalCapture")
			.a("plus.MinorModelAgeDisclosure", "http://ns.useplus.org/ldf/vocab/AG-UNK")
			.a("plus.ModelReleaseStatus", "http://ns.useplus.org/ldf/vocab/MR-UMR")
			.a("plus.PropertyReleaseStatus", "http://ns.useplus.org/ldf/vocab/PR-UPR")
			.a("plus.ImageSupplierImageID", "Supplier's Image ID")
			.a("aux.ImageNumber", "0")
			.a("photomechanic:Prefs", "-1:-1:-1:000000")
			.a("photomechanic:PMVersion", "PM5")
			.e("xmlRights:UsageTerms")
                            .e("rdf:Alt")
				.e("rdf:li")
                                    .a("xml:lang", "x-default")
                                    .t("Rights Usage Terms")
                                .up() //end rdf:li
                            .up() //end rdf:Alt
			.up() //end xmlRights.UsageTerms
			.e("dc:subject")
                            .e("rdf:Bag")
                                .e("rdf:li")
                                    .t("Keywords")
                                .up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end dc:subject
                        .e("dc.description")
                            .e("rdf:Alt")
				.e("rdf:li")
                                    .a("xml:lang", "x-default")
                                    .t("")
				.up() //end rdf:li
                            .up() //end rdf:Alt
			.up() //end dc.description
			.e("dc.creator")
                            .e("rdf:Seq")
				.e("rdf.li")
                                    .t("")
                                .up() //end rdf.li
                            .up() //end rdf.Seq
			.up() //end dc.creator
			.e("dc.title")
                            .e("rdf:Alt")
				.e("rdf:li")
                                    .a("xml:lang", "x-default")
                                    .t("")
                                .up() //end rdf:li
                            .up() //end rdf:Alt
			.up() //end dc.title
			.e("dc.rights")
                            .e("rdf:Alt")
				.e("rdf:li")
                                    .a("xml:lang", "x-default")
                                    .t("")
				.up() //end rdf:li
                            .up() //end rdf:Alt
			.up() //end dc.rights
			.e("Iptc4xmpCore:Scene")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("IPTC Scene Code")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpCore:Scene
			.e("Iptc4xmpCore:SubjectCode")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("IPTC Subject Code")
                                .up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpCore:SubjectCode
			.e("Iptc4xmpCore:CreatorContactInfo")
                            .a("Iptc4xmpCore:CiAdrExtadr", "Creator Address")
                            .a("Iptc4xmpCore:CiAdrCity", "Creator City")
                            .a("Iptc4xmpCore:CiAdrRegion", "Creator State")
                            .a("Iptc4xmpCore:CiAdrPcode", "Creator Postal Code")
                            .a("Iptc4xmpCore:CiAdrCtry", "Creator Country")
                            .a("Iptc4xmpCore:CiTelWork", "Creator Phones")
                            .a("Iptc4xmpCore:CiEmailWork", "Creator Emails")
                            .a("Iptc4xmpCore:CiUrlWork", "Creator Website")
                        .up() //end Iptc4xmpCore:CreatorContactInfo
			.e("Iptc4xmpExt:PersonInImage")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:PersonInImage
			.e("Iptc4xmpExt:LocationCreated")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .a("Iptc4xmpExt:Sublocation", "sublocation")
                                    .a("Iptc4xmpExt:City", "City")
                                    .a("Iptc4xmpExt:ProvinceState", "State")
                                    .a("Iptc4xmpExt:CountryName", "Country")
                                    .a("Iptc4xmpExt:CountryCode", "country iso")
                                    .a("Iptc4xmpExt:WorldRegion", "world region")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:LocationCreated
			.e("Iptc4xmpExt:LocationShown")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .a("Iptc4xmpExt:Sublocation", "sublocation")
                                    .a("Iptc4xmpExt:City", "City")
                                    .a("Iptc4xmpExt:ProvinceState", "State")
                                    .a("Iptc4xmpExt:CountryName", "Country")
                                    .a("Iptc4xmpExt:CountryCode", "country iso")
                                    .a("Iptc4xmpExt:WorldRegion", "world region")
                                .up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:LocationShown
			.e("Iptc4xmpExt:OrganisationInImageName")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("Name of Featured Organization")
				.up() //end rdf:li
                            .up() //end rdf:Bag
                        .up() //end Iptc4xmpExt:OrganisationInImageName
			.e("Iptc4xmpExt:OrganisationInImageCode")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("Code of Featured Organisation")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:OrganisationInImageCode
			.e("Iptc4xmpExt:Event")
                            .e("rdf:Alt")
				.e("rdf:li")
                                    .a("xml:lang", "x-default")
                                    .t("Event")
				.up() //end rdf:li
                            .up() //end rdf:Alt
			.up() //end Iptc4xmpExt:Event
			.e("Iptc4xmpExt:ModelAge")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("Model Age")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:ModelAge
			.e("Iptc4xmpExt:RegistryId")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .a("Iptc4xmpExt:RegOrgId", "Registry organisation identifier")
                                    .a("Iptc4xmpExt:RegItemId", "Registry image identifier")
                                .up() //end rdf:li
				.e("rdf:li")
                                    .a("Iptc4xmpExt:RegOrgId", "Registry organization identifier")
                                    .a("Iptc4xmpExt:RegItemId", "Registry image identifier")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:RegistryId
			.e("Iptc4xmpExt:ArtworkOrObject")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .e("rdf:Description")
					.a("Iptc4xmpExt:AODateCreated", "2001-01-01T00:00Z")
					.a("Iptc4xmpExt:AOSource", "Artwork Source")
					.a("Iptc4xmpExt:AOSourceInvNo", "Artwork Source Inventory number")
					.a("Iptc4xmpExt:AOCopyrightNotice", "Artwork Copyright notice")
                                        .e("Iptc4xmpExt:AOTitle")
                                            .e("rdf:Alt")
						.e("rdf:li")
                                                    .a("xml:lang", "x-default")
                                                    .t("Artwork Title")
						.up() //end rdf.li
                                            .up() //end rdf.Alt
					.up() //end Iptc4xmpExt:AOTitle
                                        .e("Iptc4xmpExt:AOCreator")
                                            .e("rdf:Seq")
						.e("rdf.li")
                                                    .t("ArtworkCreator")
						.up() //end rdf.li
                                            .up() //end rdf.Seq
					.up() //end Iptc4xmpExt:AOCreator
                                    .up() //end rdf:Description
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end Iptc4xmpExt:ArtworkOrObject
			.e("plus.ModelReleaseID")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("Model Release Identifier")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end plus.ModelReleaseID
			.e("plus.ImageCreator")
                            .e("rdf:Seq")
				.e("rdf.li")
                                    .a("plus.ImageCreatorName", "Image Creator Name")
                                    .a("plus.ImageCreatorID", "Image Creator Identifier")
				.up() //end rdf.li
                            .up() //end rdf.Seq
			.up() //end plus.ImageCreator
			.e("plus.CopyrightOwner")
                            .e("rdf:Seq")
				.e("rdf.li")
                                    .a("plus.CopyrightOwnerName", "Copyright Owner Name")
                                    .a("plus.CopyrightOwnerID", "Copyright Owner Identifier")
				.up() //end rdf.li
                            .up() //end rdf.Seq
			.up() //end plus.CopyrightOwner
			.e("plus.PropertyReleaseID")
                            .e("rdf:Bag")
				.e("rdf:li")
                                    .t("Property Release ID")
				.up() //end rdf:li
                            .up() //end rdf:Bag
			.up() //end plus.PropertyReleaseID
			.e("plus.ImageSupplier")
                            .e("rdf:Seq")
				.e("rdf.li")
                                    .a("plus.ImageSupplierName", "Image Supplier Name")
                                    .a("plus.ImageSupplierID", "Image Supplier ID")
				.up() //end rdf.li
                            .up() //end rdf.Seq
			.up() //end plus.ImageSupplier
                    .up()//end rdf:Description
		.up() //end rdf:RDF
            .up(); //end x:xmpmeta
 
            
        }
        catch (Exception e) {
			// TODO Auto-generated catch block
		e.printStackTrace();
        }
        
    }
    
    public boolean create (CDIS cdis_new, String tmsFileName, XMLBuilder xml) {
       
        try {
            //sometimes we have forward or back slashes or a combination of both in TMS
            //Replace all the forward slashes with backslashes, 
            if (tmsFileName.contains("/")) {
                logger.log(Level.FINER, "FileName: " + tmsFileName);
                tmsFileName = tmsFileName.replace("/", "\\");
            }
        
            String pathlessFileName = null;
    
            // Get the filename stripped of the path
            if (tmsFileName.contains("\\")) {
                pathlessFileName = tmsFileName.substring(tmsFileName.lastIndexOf("\\"));
            }
            else {
                pathlessFileName = tmsFileName;
            }
    
            // remove .jpg and .tif extension
            pathlessFileName = pathlessFileName.replace(".jpg","");
            pathlessFileName = pathlessFileName.replace(".tif","");
            
            // Create the String holding the whole path and filename
            String xmlMetaFileLocation = cdis_new.properties.getProperty("workFolder") + "\\" + pathlessFileName + ".xml";
        
            logger.log(Level.FINER, "Creating Work Folder: " + xmlMetaFileLocation);
        
            //Create and write to the new metaDataXML file
            File metaDataXmlFile = new File(xmlMetaFileLocation);
            Properties outputProperties = new Properties();
            outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
        
        
            FileWriter xmlWriter = new FileWriter(metaDataXmlFile);
            xml.toWriter(xmlWriter, outputProperties);
        
            }catch (Exception e) {
		e.printStackTrace();
                return false;
            }
        
            return true;
    }
                
}
