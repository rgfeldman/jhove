/**
 CDIS 2.0 - Smithsonian Anacostia Museum (ACM) Code Branch
 CDIS.java
 */
package edu.si.dams;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.xml.transform.TransformerException;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import mediaTools.bag.BagException;
import mediaTools.bag.Bagger;
import mediaTools.bag.ChecksumException;
import mediaTools.bag.ChecksumUtils;

import com.artesia.common.utils.DateUtils;
import com.jamesmurty.utils.XMLBuilder;

import edu.si.data.DataProvider;
import edu.si.tms.TMSMediaRendition;
import edu.si.tms.audit.AuditTrailReader;
import edu.si.tms.factory.TMSMediaRenditionFactory;

/**
 * @author davisch
 *
 */
public class CDIS {
	
	public Logger _log = null;
	
	private Properties properties = new Properties();
	
	private static final String[] requiredProps = {"damsDriver",
													"damsUrl",
													"damsUser",
													"damsPass",
													"tmsDriver",
													"tmsUrl",
													"tmsUser",
													"tmsPass",
													"workFolder",
													"hotFolder",
													"bagIt",
													"mediaDrive"};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Connection tmsConn, damsConn;
		
		CDIS ingester = new CDIS();
		
                // Set up logger
                ingester._log = Logger.getLogger(ingester.getClass().getName());
		ingester._log.setLevel(Level.ALL);
		Handler fh = null;
		DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
		try {
			//fh = new FileHandler("TDIS.log");
			fh = new FileHandler("TDIS-" + ingester.properties.getProperty("operationType") + ".log." + df.format(new Date()));
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			ingester._log.log(Level.SEVERE, "SecurityException in main(): {0}", e.getMessage());
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			ingester._log.log(Level.SEVERE, "IOException in main(): {0}", e.getMessage());
			//e.printStackTrace();
		}
		fh.setFormatter(new SimpleFormatter());
		ingester._log.addHandler(fh);
		
                ingester._log.log(Level.ALL, "Logging Established");
                fh.flush();
                
		//validate configuration file.
		String configFileName = new String();
		if(args.length < 1) {
			System.out.println("Missing parameter: <configFileName>");
                        ingester._log.log(Level.ALL, "Missing parameter: <configFileName>");
			return;
		}
		else {
			configFileName = args[0];
		}
		
		try {
			ingester.loadProperties(configFileName);
		}
		catch(FileNotFoundException fnfe) {
			//ingester._log.log(Level.ALL, "Config file path invalid.");
			System.out.println("Config file path invalid.");
			return;
		}
		catch(Exception ex) {
			ex.printStackTrace();
			//ingester._log.log(Level.ALL, "Config file missing parameter: " + ex.getMessage());
			System.out.println("Exception caught while parsing config file: " + ex.getMessage());
			return;
		}
		
		
                
                
		ingester._log.log(Level.ALL, "Configuration file verified.");
		//ingester._log.log(Level.INFO, "Configuration file verified. Switching to operation log.");
		//ingester._log.removeHandler(fh);
		//fh.close();
		
		//establish and verify database connections.
		try {
			tmsConn = DataProvider.getConnection(ingester.properties.getProperty("tmsDriver"), 
												ingester.properties.getProperty("tmsUrl"), 
												ingester.properties.getProperty("tmsUser"), 
												ingester.properties.getProperty("tmsPass"));
		}
		catch(Exception ex) {
			ingester._log.log(Level.ALL, "There was a problem connecting to the TMS database: {0}", ex.getMessage());
			//ex.printStackTrace();
			return;
		}
		ingester._log.log(Level.ALL, "Connection to TMS database established.");
		
		
		//establish and verify database connections.
		try {
			damsConn = DataProvider.getConnection(ingester.properties.getProperty("damsDriver"), 
					ingester.properties.getProperty("damsUrl"), 
					ingester.properties.getProperty("damsUser"), 
					ingester.properties.getProperty("damsPass"));
		}
		catch(Exception ex) {
			ingester._log.log(Level.ALL, "There was a problem connecting to the DAMS database: {0}", ex.getMessage());
			//ex.printStackTrace();
			return;
		}
		ingester._log.log(Level.ALL, "Connection to DAMS database established.");
		
		ArrayList<String> newRenditions = new ArrayList<String>();
			ArrayList<String> newlySyncedRenditions = new ArrayList<String>();
			
			ArrayList<String> metadataAssets = new ArrayList<String>();
			ArrayList<String> failedMetadata = new ArrayList<String>();
			ArrayList<String> IDSAssets = new ArrayList<String>();
			ArrayList<String> failedIDS = new ArrayList<String>();
			ArrayList<String> ingestedFromDAMS = new ArrayList<String>();
			
			ArrayList<String> unsyncedRenditions = new ArrayList<String>();
			
			if(ingester.properties.getProperty("operationType").equals("ingest")) {
				
				//if ingestFromDAMS = true, create media records for applicable DAMS assets
				if(ingester.properties.getProperty("ingestFromDAMS") != null && ingester.properties.getProperty("ingestFromDAMS").equals("true")) {
					//get UOIIDS, UANs for applicable assets
					HashMap<String, String> newAssets = ingester.retrieveNewAssets(damsConn);
					
					ingester._log.log(Level.ALL, "newAssets: {0}", newAssets.size());
					
					//for each
					for(Iterator<String> iter = newAssets.keySet().iterator(); iter.hasNext();) {
						String UOIID = iter.next();
						String UAN = newAssets.get(UOIID);
						
						//pull necessary parameters for media records stored proc
						//pull objectNumber and Rank
						HashMap<String, String> objectInfo = ingester.getObjectInfoForAsset(damsConn, UOIID);
						if(objectInfo != null) {
							String objectNumber = objectInfo.keySet().toArray()[0].toString();
							String rank = objectInfo.get(objectNumber);
							String objectID = ingester.getObjectIDForAsset(tmsConn, objectNumber);
							ingester._log.log(Level.ALL, "UOIID: {0}", UOIID);
							ingester._log.log(Level.ALL, "UAN: {0}", UAN);
							ingester._log.log(Level.ALL, "IDSPathID: {0}", ingester.properties.getProperty("IDSPathId"));
							ingester._log.log(Level.ALL, "ObjectID: {0}", objectID);
							ingester._log.log(Level.ALL, "Rank: {0}", rank);
							
							String title = "acmobj-" + objectNumber.replaceAll("\\.", "") + "-r" + rank;
							
							//check if UAN already exists for a MediaFile in TMS
							if(ingester.doesUANExistInTMS(tmsConn, UAN)) {
								//asset file is already being pointed to in TMS, just fix the Rendition Number, add to TDIS table
								ingester.replaceTMSRenditionNumber(tmsConn, damsConn, UAN, title, UOIID);
								
								ingester._log.log(Level.ALL, "Rendition {0} successfully updated in TMS.", title);
							}
							else {
						
								//call media records stored proc
								//check parameters
								if(objectID != null) {
									ingester.createMediaRecords(tmsConn, UOIID, UAN, ingester.properties.getProperty("IDSPathId"), title, objectID, rank);
									
									//create BLOB for thumbnail
									URL assetURL;
									try {
										assetURL = new URL("http://ids-internal.si.edu/ids/deliveryService/id/" + UAN + "/192");
										InputStream UANStream = assetURL.openStream();
										byte[] bytes = IOUtils.toByteArray(UANStream);
										
										PreparedStatement stmt = tmsConn.prepareStatement("update MediaRenditions set ThumbBLOB = ? where RenditionID = (select RenditionID from TDIS where UOIID = ?)");
										stmt.setBytes(1, bytes);
										stmt.setString(2, UOIID);
										
										DataProvider.executeUpdate(tmsConn, stmt);
										
										UANStream.close();
									} catch (MalformedURLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "There was a problem retrieving asset with UAN {0} from IDS. No thumbnail will be saved in the database for this asset.", UAN);
									} catch (IOException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "IOException when creating thumbnail for asset {0}. Skipping...", UAN);
									} catch (SQLException e) {
										// TODO Auto-generated catch block
										ingester._log.log(Level.ALL, "SQLException when updating thumbnail for asset {0}. Skipping...", UAN);
									}
									
									//update DAMS.SOURCE_SYSTEM_IDENTIFIER with rendition number
									ingester.updateDAMSAsset(damsConn, UOIID, title);
									ingester._log.log(Level.ALL, "Updated: {0}", title);
									ingestedFromDAMS.add(title);
								}
								
								
							}
						}
						
					}
					ingester._log.log(Level.ALL, "Finished iterating.");
					
					
				}
			
				//pull recently created TMS records.
				newRenditions = ingester.retrieveNewRenditions(tmsConn);
				
				//pull TDIS records with no sync - failed transfers
				newRenditions.addAll(ingester.retrieveFailedTransfers(tmsConn));
				//get newly flagged TMS records, check for duplicates with newly created records
				for(Iterator<String> iter = ingester.getRecentlyFlaggedRenditions(tmsConn).iterator(); iter.hasNext();) {
					String tempString = iter.next();
					if(!newRenditions.contains(tempString)) {
						newRenditions.add(tempString);
					}
				}
				//check for any other derivatives for these files
				/*for(Iterator<String> iter = newRenditions.iterator(); iter.hasNext();) {
					
				}*/
				//newRenditions.addAll(ingester.getRecentlyFlaggedRenditions(tmsConn));
				ingester._log.log(Level.ALL, "Number of new renditions to migrate: {0}", newRenditions.size());
				if(newRenditions.isEmpty()) {
					ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
					boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
					
					if(!success) {
			        	ingester._log.log(Level.ALL, "There was an error creating the log entry.");
			        }
					ingester._log.log(Level.ALL, "Exiting...");
					return;
				}
			}
			else if(ingester.properties.getProperty("operationType").equals("sync")) { //operationType=sync
				
				//clear out old log files - older than 30 days
				//FileUtils.isFileOlder(;, date);
				File folderDir = new File(".");
				DateFormat df1 = new SimpleDateFormat("MM-dd-yyyy");
				File[] logs = folderDir.listFiles();
				
				for(int i = 0; i < logs.length; i++) {
					File tempFile = logs[i];
					if(tempFile.getName().startsWith("TDIS")) {
						//get 30 days before today
						Calendar thirtyBack = Calendar.getInstance();
						thirtyBack.add(Calendar.DAY_OF_MONTH, -30);
						Calendar fileCal = Calendar.getInstance();
						fileCal.setTimeInMillis(tempFile.lastModified());
						if(thirtyBack.after(fileCal)) {
							//delete log file
							boolean deleted = tempFile.delete();
							if(deleted) {
								ingester._log.log(Level.ALL, "Log file {0} successfully deleted.", tempFile.getName());
							}
							else {
								ingester._log.log(Level.ALL, "There was an error deleting log file {0}.", tempFile.getName());
							}
						}
						
					}
						
				}
				
				//find un-synced renditions
				unsyncedRenditions = ingester.retrieveUnsyncedRenditions(tmsConn);
				
				if(!unsyncedRenditions.isEmpty()) {
					for(Iterator<String> iter = unsyncedRenditions.iterator(); iter.hasNext();) {
						boolean result = ingester.unsyncRendition(iter.next(), tmsConn, damsConn);
					}
				}
				
				//find renditions with primary file change
				/*ArrayList<String> newPrimaryRenditions = ingester.retrievePrimaryFileChanges(tmsConn);
				
				if(!newPrimaryRenditions.isEmpty()) {
					for(Iterator<String> iter = newPrimaryRenditions.iterator(); iter.hasNext();) {
						String rendition = iter.next();
						boolean result = ingester.unsyncRendition(rendition, tmsConn, damsConn);
						unsyncedRenditions.add(rendition);
						//these renditions will be picked up for re-sync next time the ingest service runs
					}
				}*/
				
				//find renditions ready for IDS sync
				//list of UOIIDS requiring IDS file path sync
				ArrayList<String> renditionsRequiringIDSPath = ingester.renditionsForIDSSync(tmsConn);
				//ingester._log.log(Level.ALL, "Renditions requiring IDS Path: " + renditionsRequiringIDSPath.size());
				
				if(!renditionsRequiringIDSPath.isEmpty()) {
					//find UOIID and UAN mapping for matching assets
					HashMap<String, String> UOIIDSandUANS = ingester.getUANPairs(damsConn, tmsConn, renditionsRequiringIDSPath);
					//ingester._log.log(Level.ALL, "UOIID/UAN Pairs: " + UOIIDSandUANS.size());
					
					for(Iterator<String> iter = UOIIDSandUANS.keySet().iterator(); iter.hasNext();) {
						String UOIID = iter.next();
						String UAN = UOIIDSandUANS.get(UOIID);
						boolean result = ingester.syncFilePath(tmsConn, UOIID, UAN);
						if(result) {
							ingester._log.log(Level.ALL, "Asset {0} successfully linked with IDS derivative.", UOIID);
							IDSAssets.add(UOIID);
						}
						else {
							ingester._log.log(Level.ALL, "There was an error linking asset {0} with IDS derivative. Skipping...", UOIID);
							failedIDS.add(UOIID);
						}
					}
				}
							
				
				//retrieve UOIIDs of newly ingested objects
				ArrayList<String> fileNames = ingester.retrieveRenditionsPendingSync(tmsConn);
				
				if(fileNames != null && fileNames.size() > 0) {
					HashMap<String, String> syncPairs = ingester.retrieveUOIIDSForSync(damsConn, fileNames);
					
					if(syncPairs != null && !syncPairs.isEmpty()) {
						//update TDIS with new UOIIDs
						boolean result = ingester.syncNewRecords(tmsConn, damsConn, syncPairs);
						
						//add renditionIDs to newlySyncedRenditions
						newlySyncedRenditions = ingester.getRenditionsForFileNames(tmsConn, syncPairs);
					}
				}
				else {
					ingester._log.log(Level.ALL, "No new assets to sync UOIIDs with DAMS. Continuing...");
				}
	
				
				//pull recently updated TMS records.
				newRenditions = ingester.retrieveUpdatedRenditions(tmsConn);
				
				//add newly synced renditions
				if(newlySyncedRenditions != null && !newlySyncedRenditions.isEmpty()) {
					for(Iterator<String> iter = newlySyncedRenditions.iterator(); iter.hasNext();) {
						String tempRendition = iter.next();
						if(!newRenditions.contains(tempRendition)) {
							newRenditions.add(tempRendition);
						}
					}
					
				}
				
				//pull newly created assets ingested from DAMS
				//EnteredDate >= LastRan, PathID = IDSPathId, IsColor = 1
				ArrayList<String> DAMSIngestedAssets = ingester.getAssetsIngestedfromDAMS(tmsConn);
				
				if(DAMSIngestedAssets != null && !DAMSIngestedAssets.isEmpty()) {
					for(Iterator<String> iter = DAMSIngestedAssets.iterator(); iter.hasNext();) {
						String tempRendition = iter.next();
						if(!newRenditions.contains(tempRendition)) {
							newRenditions.add(tempRendition);
						}
					}
					
				}
				
				
				
				ingester._log.log(Level.ALL, "Number of new renditions to migrate: {0}", newRenditions.size());
				if(newRenditions.isEmpty()) {
					ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
					boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
					
					if(!success) {
			        	ingester._log.log(Level.ALL, "There was an error creating the log entry.");
			        }
					ingester._log.log(Level.ALL, "Exiting...");
				}
			}
			
			if(ingester.properties.getProperty("operationType").equals("purge")) { //operationType=purge
				//determine which assets are eligible for purging
				//criteria is: synced with DAMS, path set to IDS
				
				HashMap<String, String> pathsAndFiles = ingester.getPurgeable(tmsConn, ingester.properties.get("IDSPathId").toString());  
				
				if(pathsAndFiles != null) {
					//cycle through the paths and files
					for(Iterator<String> iter = pathsAndFiles.keySet().iterator(); iter.hasNext();) {
						//create the full path
						String key = iter.next();
						String fullPath = key + "\\" + pathsAndFiles.get(key);
						ingester._log.log(Level.ALL, "File {0} to be deleted.", fullPath);
						File purgeFile = new File(fullPath);
						//boolean result = purgeFile.delete();
						boolean result = true;
						if(result) {
							ingester._log.log(Level.ALL, "File: {0} successfully deleted.", fullPath);
							//set TDIS.Deleted = 'Yes'
							boolean deleted = ingester.markAsDeleted(tmsConn, key, pathsAndFiles.get(key));
							
						}
						else {
							ingester._log.log(Level.ALL, "There was an error deleting file: {0}.", fullPath);
						}
						
					}
				}
				else {
					ingester._log.log(Level.ALL, "There was an error during the purge process. Skipping...");
				}
				
				
				
			}
			
			//populate data objects from TMS data.
			ArrayList<TMSMediaRendition> renditionObjects = ingester.loadTMSData(tmsConn, newRenditions, ingester.properties.getProperty("operationType"));
			ingester._log.log(Level.ALL, "Populated data objects from TMS data.");
			
			if(ingester.properties.get("bagIt").equals("true")) {
				//initiate bag creation
				//bagName = new Date().getTime()+"";
				
				//ingester._log.log(Level.ALL, "  creating bag " + bagName + " for " + fileName);
				//item = ingester.getHoleyBagData(tempRendition, tmsConn);
		        //File bag = ingester.buildBag(ingester._log, tmsConn, hotFolder, bagName, tempRendition, item, xml);
				
				 // If we've gotten this far we've done all we can do successfully.
		        // the rest is up to the artesia ingester on the DAM.  Log OK:
		        //logdata.setBagname(bagname);
		        //logdata.setEnd(new Date());
		        //logdata.setStatus(DBLogger.OK);
		        //dbLogger.write(conn, logdata, logDS);
				
				//} catch (BagException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			//} catch (ChecksumException e) {
				// TODO Auto-generated catch block
			//	e.printStackTrace();
			//} 
			}
			
			
			File hotFolder = new File(ingester.properties.getProperty("hotFolder"));
			File workFolder = new File(ingester.properties.getProperty("workFolder"));
			ArrayList<String> successAssets = new ArrayList<String>();
			ArrayList<String> failedAssets = new ArrayList<String>();
			HoleyBagData item;
			String bagName;
			
			try {
				for(Iterator<TMSMediaRendition> iter = renditionObjects.iterator(); iter.hasNext();) {
					TMSMediaRendition tempRendition = iter.next();
					
					if(ingester.properties.getProperty("bagIt").equals("false")) {
						String fileName = new String();
						if(tempRendition.getName().contains("\\")) {
							fileName = tempRendition.getName().split("\\\\")[tempRendition.getName().split("\\\\").length-1];
						}
						else {
							fileName = tempRendition.getName();
						}
						////system.out.println("Filename: " + fileName);
						XMLBuilder xml = tempRendition.getMetadataXMP();
						
						String filePath = tempRendition.getStructuralPath() + "\\" + tempRendition.getName();
						
						if(ingester.properties.getProperty("operationType").equals("ingest")) {
							//copy file to hotfolder
							boolean result = ingester.placeInHotFolder(hotFolder, tempRendition, xml, workFolder);
							if(result) {
								ingester._log.log(Level.ALL, "Asset {0} successfully moved to hotfolder at location {1}", new Object[]{tempRendition.getFileName(), hotFolder.getAbsolutePath()});
								successAssets.add(tempRendition.getFileName());
							}
							else {
								ingester._log.log(Level.ALL, "Error occurred while moving asset {0}. Skipping...", tempRendition.getFileName());
								failedAssets.add(tempRendition.getFileName());
							}
							
							//check if asset has a TIFF associated with it
							if(!tempRendition.getName().endsWith("tif")) {
								String tiffPath = ingester.convertMediaPath(tempRendition.getStructuralPath()) + "\\" + tempRendition.getName().replace("jpg", "tif");
								File tiff = new File(tiffPath);
								if(tiff.exists()) {
									//high resolution file available
									//copy file to hotfolder
									//move asset file to workfolder
							    	Scanner scanner;
								    File destFile = new File(workFolder.getAbsolutePath() + "\\" + tempRendition.getName().replace("jpg", "tif"));
								    //System.out.println("Copying from " + tiffPath);
								    ingester._log.log(Level.ALL, "Beginning file copy to work folder...");
								    ingester._log.log(Level.ALL, "Source file size: {0}", tiff.length());
								    boolean isCopying = true;
								    try {
								    	FileUtils.copyFile(tiff, destFile);
									    /*while(isCopying) {
									    	try {
								                scanner = new Scanner(destFile);
								                isCopying = false;
								            } catch (FileNotFoundException e) {
								                System.out.println("File not found or is in copy State. ");
								                try {
													Thread.sleep(100);
												} catch (InterruptedException e1) {
													// TODO Auto-generated catch block
													e1.printStackTrace();
												}
								            }
									    }*/
									    ingester._log.log(Level.ALL, "File copy to work folder complete.");
									    ingester._log.log(Level.ALL, "Beginning file copy to hot folder...");
									    //if successful, copy both files to hotfolder
									    
										FileUtils.copyFile(destFile, new File(hotFolder.getAbsolutePath() + "\\" + "MASTER" + "\\" + tempRendition.getName().replace("jpg", "tif")));
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								    
								    ingester._log.log(Level.ALL, "File copy to hot folder complete.");
									
								}
							}
						}
						
						if(ingester.properties.getProperty("operationType").equals("sync")) {
							//grab UOIID for rendition
							String UOIID = ingester.getUOIIDForRendition(tmsConn, tempRendition);
							if(UOIID != null) {
								//manually make metadata updates for changed renditions
								boolean result = ingester.updateMetadataManually(damsConn, tempRendition, UOIID);
								
								if(result) {
									ingester._log.log(Level.ALL, "Successfully synced metadata changes for rendition {0}", tempRendition.getName());
									metadataAssets.add(tempRendition.getName());
								}
								else {
									ingester._log.log(Level.ALL, "There was an error syncing metadata changes for rendition {0}. Skipping...", tempRendition.getName());
									failedMetadata.add(tempRendition.getName());
								}
	
								//check if asset requires IDS sync in TMS
								//boolean tmsCheck = ingester.requiresIDSSync(tmsConn, tempRendition);
								
								//check if available for sync in DAMS
								//if(tmsCheck) {
								//	String UAN = ingester.eligibleForSync(damsConn, UOIID);
								
									//if both, make the sync
								//	if(UAN != null) {
								//		boolean result = ingester.syncFilePath(tmsConn, tempRendition, UAN);
										
								//		if(!result) {
								//			ingester._log.log(Level.ALL, "There was an error updating the file path information for rendition " + tempRendition.getRenditionNumber() + ".");
								//		}
								//	}
								//}
							}
							
							
						}
					}
					else { //bagIt = true
						//bag stuff here, will fill in later
						
					}
					
	
			        ingester._log.log(Level.ALL, "Creating updated record for rendition: {0}", tempRendition.getRenditionNumber());
			        boolean success = ingester.createIngestRecord(tmsConn, tempRendition);
			        
			        if(!success) {
			        	ingester._log.log(Level.ALL, "There was an error creating the ingest record for rendition: {0}", tempRendition.getRenditionNumber());
			        }
	
			       
					
				}
				
				ingester._log.log(Level.ALL, "Creating email report for {0} operation...", ingester.properties.getProperty("operationType"));
				boolean emailResult = false;
				if(ingester.properties.getProperty("operationType").equals("ingest")) {
					//create ready.txt in hotfolder
					File readyFile = new File(hotFolder.getAbsolutePath() + "\\" + "MASTER" + "\\ready.txt");
					try {
						readyFile.createNewFile();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						ingester._log.log(Level.ALL, "An error occurred when creating the ready.txt file. Ingestion will occur the next time the ingest process is run nightly.");
					}
					if(!successAssets.isEmpty() || 
								!failedAssets.isEmpty() || !ingestedFromDAMS.isEmpty()) {
						emailResult = ingester.sendIngestEmail(successAssets, failedAssets, ingestedFromDAMS);
					}
				}
				else if(ingester.properties.getProperty("operationType").equals("sync")) {
					if(!metadataAssets.isEmpty() || 
							!failedMetadata.isEmpty() || 
							!IDSAssets.isEmpty() ||
							!failedIDS.isEmpty() ||
							!unsyncedRenditions.isEmpty()) {
						emailResult = ingester.sendSyncEmail(metadataAssets, IDSAssets, failedMetadata, failedIDS, unsyncedRenditions);
					}
				}
				
		
		
			ingester._log.log(Level.ALL, "Creating log entry for {0} operation...", ingester.properties.getProperty("operationType"));
			boolean success = ingester.createLogEntry(tmsConn, ingester.properties.getProperty("operationType"));
			
			if(!success) {
	        	ingester._log.log(Level.ALL, "There was an error creating the log entry.");
	        }
			
		
		

		
		//try {
			//set UOIID in TMS
			/*if(ingester.properties.getProperty("operationType").equals("sync")) {
				//check for synced assets with no UOIID
				ArrayList<String> renditionsForUAN = getRenditionsForUAN(tmsConn);
				
				//for each rendition pulled, check if there's an asset in DAMS with a valid UAN
				for(Iterator<String> iter = renditionsForUAN.iterator(); iter.hasNext();) {
					String renditionNumber = iter.next();
					
					boolean synced = ingester.syncIfNeeded(tmsConn, damsConn, renditionNumber);
					
					
				}
			}*/
		
		
			tmsConn.close();
			damsConn.close();
		}
		catch(SQLException sqlex) {
			ingester._log.log(Level.ALL, "Exception in main(): {0}", sqlex.getMessage());
			//sqlex.printStackTrace();
		}	

	}

	private void replaceTMSRenditionNumber(Connection tmsConn, Connection damsConn, String UAN,
			String title, String UOIID) {
		
		//make sure FileName is unique
		
		String uniqueQuery = "select count(*) from MediaFiles where FileName = '" + UAN + "'";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, uniqueQuery);
		
		boolean unique = false;
		
		try {
			while(rs.next()) {
				int count = rs.getInt(1);
				if(count == 1) {
					unique = true;
				}
			}
			rs.close();
		} catch (SQLException e) {
			_log.log(Level.ALL, "SQLException in replaceTMSRenditionNumber: {0}", e.getMessage());
		}
		
		if(unique) {
			String query = "update MediaRenditions set RenditionNumber = '" + title + "', IsColor = 1 where " +
					"RenditionID = (select RenditionID from MediaFiles where FileName = '" + UAN + "')";
			
			//_log.log(Level.ALL, "replaceTMSRenditionNumber query: " + query);
			
			DataProvider.executeUpdate(tmsConn, query);
			
			//create TDIS record
			//grab Rendition information
			query = "select RenditionID, RenditionNumber from MediaRenditions where RenditionID = " +
					"(select RenditionID from MediaFiles where FileName = '" + UAN + "')";
			
			rs = DataProvider.executeSelect(tmsConn, query);
			int renditionID = 0;
			String renditionNumber = new String();
			
			try {
				while(rs.next()) {
					renditionID = rs.getInt(1);
					renditionNumber = rs.getString(2);
				}
				
				rs.close();
			} catch (SQLException e) {
				_log.log(Level.ALL, "SQLException caught in replaceTMSRenditionNumber: {0}", e.getMessage());
			}
			
			query = "insert into TDIS(RenditionID, RenditionNumber, UOIID)values(" +
					renditionID + "," +
						"'" + renditionNumber + "'," + 
						"'" + UOIID + "')";
			
			DataProvider.executeInsert(tmsConn, query);
			
			//update SOURCE_SYSTEM_ID in DAMS.SI_ASSET_METADATA
			query = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + renditionNumber + "' where UOI_ID = '" + UOIID + "'";
			
			
			DataProvider.executeUpdate(damsConn, query);
			
			
		}
		else {
			_log.log(Level.ALL, "More than one Media File exists with filename {0}. Skipping...", UAN);
		}
		
	}

	private boolean doesUANExistInTMS(Connection tmsConn, String UAN) {
		
		String query = "select count(*) from MediaFiles where FileName = '" + UAN + "'";
		boolean retval = false;
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, query);
		
		try {
			while(rs.next()) {
				int count = rs.getInt(1);
				if(count > 0) {
					retval = true;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "There was an exception during doesUANExistInTMS, asset with UAN: {0}. Skipping...", UAN);
		}
		finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return retval;
	}

	private ArrayList<String> getAssetsIngestedfromDAMS(Connection tmsConn) {
		
		ArrayList<String> retval = new ArrayList<String>();
		
		String sql = "select RenditionID from MediaRenditions where " +
				"EnteredDate >= (select top 1 LastRan from TDIS_Log where OperationType = 'sync' order by LastRan desc) AND " +
				"RenditionID in (select RenditionID from TDIS where OriginalFilePath = '')";
		
		try {
			ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
			
			while(rs.next()) {
				retval.add(rs.getString(1));
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "getAssetsIngestedFromDAMS SQL: {0}", sql);
			e.printStackTrace();
		}
		
		return retval;
	}

	private boolean updateDAMSAsset(Connection damsConn, String UOIID, String title) {
		
		String sql = "update SI_ASSET_METADATA set SOURCE_SYSTEM_ID = '" + title + "' where UOI_ID = '" + UOIID + "'";
		System.out.println("updateDAMSAsset query: " + sql);
		return (DataProvider.executeUpdate(damsConn, sql) == 1);

		
	}

	private HashMap<String, String> getObjectInfoForAsset(Connection damsConn,
			String UOIID) {
				
			//object Number = assetTitle demarcated xxxx.xxxx.xxxx
			//ex acmobj-199100760024-r1 --> object number: 1991.0076.0024, rank=1 (base zero)
			
			HashMap<String, String> retval = new HashMap<String, String>();
			
			//get title of asset from DAMS
			String damsSQL = "select NAME from UOIS where UOI_ID = '" + UOIID + "'";
			String title = new String();
			ResultSet rs = null;
			PreparedStatement stmt = null;
			
			try {
				stmt = damsConn.prepareStatement(damsSQL);
				rs = DataProvider.executeSelect(damsConn, stmt);
				
				while(rs.next()) {
					title = rs.getString(1);
					_log.log(Level.ALL, "NAME: {0}", title);
					String rank = new String();
					if(title.split("-").length > 2) {
						rank = title.split("-")[2].split("\\.")[0].replace("r", "");
					}
					else {
						rank = "0";
					}
					//grab first 12 characters
					title = title.split("-")[1].substring(0, 12);
					retval.put(title.substring(0, 4) + "." + title.substring(4, 8) + "." + title.substring(8, 12), rank);
				}
			} catch (SQLException e) {
				_log.log(Level.ALL, "getObjectIDForAsset SQL: {0}", damsSQL);
				e.printStackTrace();
			} catch (Exception e) {
				_log.log(Level.ALL, "There was an exception processing asset {0}. Skipping...", title);
				retval = null;
			}
			finally {
				try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
				try { if (stmt != null) stmt.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
			}
			
			
			return retval;
	}

	private String getObjectIDForAsset(Connection tmsConn, String objectNumber) {
		
		String objectID = null;
		
		String sql = "select ObjectID from Objects where ObjectNumber = '" + objectNumber + "'";
		
		ResultSet rs = null;
		
		try {
			rs = DataProvider.executeSelect(tmsConn, sql);
			
			while(rs.next()) {
				objectID = rs.getString(1);
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "getObjectIDForAsset SQL: {0}", sql);
			e.printStackTrace();
		} finally {
			try { if (rs != null) rs.close(); } catch (SQLException se) { _log.log(Level.ALL, "Error closing the statement {0}", se.getMessage()); }
		}
		
		return objectID;
		
	}

	private boolean createMediaRecords(Connection tmsConn, String UOIID, String UAN, String IDSPathID, String renditionNumber, String objectID, String rank) {

		//call dbo.CreateMediaRecords
		//@UOIID, @UAN, @IDSPathID, @RenditionNumber, @ObjectID, @Rank
		
		try {
			CallableStatement stmt = tmsConn.prepareCall("{ call CreateMediaRecords(?,?,?,?,?,?)}");
			
			stmt.setString(1, UOIID);
			stmt.setString(2, UAN);
			stmt.setInt(3, Integer.parseInt(IDSPathID));
			stmt.setString(4, renditionNumber);
			stmt.setInt(5, Integer.parseInt(objectID));
			stmt.setInt(6, Integer.parseInt(rank));
			
			return (stmt.executeUpdate() != 0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return false;
	}

	private HashMap<String, String> retrieveNewAssets(Connection damsConn) {
		
		//retrieve DAMS assets in the given category that have not been synced with TMS
		
		HashMap<String, String> retval = new HashMap<String, String>();
		String sql = "select a.UOI_ID, b.OWNING_UNIT_UNIQUE_NAME from UOIS a, SI_ASSET_METADATA b, SI_IDS_EXPORT c " +
				"WHERE a.UOI_ID = b.UOI_ID " +
				"AND a.CONTENT_STATE = 'NORMAL' " +
				"AND a.CONTENT_TYPE != 'SHORTCUT' " +
				"AND b.SOURCE_SYSTEM_ID is null " +
				"AND a.UOI_ID = c.UOI_ID " +
				"AND a.UOI_ID in (select UOI_ID from NODES_FOR_UOIS where NODE_ID = '" + this.properties.getProperty("categoryNodeId") + "')" + 
				"AND a.UOI_ID = '8ed932fa984b7467817f031f2f8fd50a35035079'";
		
		ResultSet rs = DataProvider.executeSelect(damsConn, sql);
		
		try {
			while(rs.next()) {
				retval.put(rs.getString(1), rs.getString(2));			
			}
		} catch (SQLException e) {
			_log.log(Level.ALL, "retrieveNewAssets SQL: {0}", sql);
			e.printStackTrace();
		}
		
		return retval;
	}

	private ArrayList<String> retrieveFailedTransfers(
			Connection tmsConn) {
		//retrieve TDIS records that still have '-1' for UOIID
		
		ArrayList<String> retval = new ArrayList<String>();
		String sql = "select RenditionID from TDIS where UOIID = '-1'";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		
		try {
			while(rs.next()) {
				retval.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retval;
	}

	//UOIID, NAME
	private HashMap<String, String> getMatchingAssets(Connection damsConn,
			Connection tmsConn) {
		
		ArrayList<String> fileNames = new ArrayList<String>();
		HashMap<String, String> pairs = new HashMap<String, String>();
		
		//find Renditions that aren't present in TDIS table
		String query = "select b.FileName " +
				"from MediaRenditions a, MediaFiles b " +
				"where a.PrimaryFileID = b.FileID AND " +
				"a.RenditionID not in (select RenditionID from TDIS) " +
				"order by b.FileName";
		
		PreparedStatement stmt;
		
		try {
			stmt = tmsConn.prepareStatement(query);
			ResultSet rs = DataProvider.executeSelect(tmsConn, stmt);
			int count = 0;
			while(rs.next() && count < 999) {
				fileNames.add(rs.getString(1));
				count++;
			}
			
			
			String damsQuery = "select UOI_ID, NAME from UOIS where NAME in (";
			
			for(Iterator<String> iter = fileNames.iterator(); iter.hasNext();) {
				damsQuery += "'" + iter.next() + "'";
				if(iter.hasNext()) {
					damsQuery += ", ";
				}
			}
			damsQuery += ")";
			
			rs = DataProvider.executeSelect(damsConn, damsQuery);
			
			while(rs.next()) {
				pairs.put(rs.getString(1), rs.getString(2));
			}
			
			//create TDIS records
			//grab RenditionID, FilePath, FileName
			for(Iterator<String> damsIter = pairs.keySet().iterator(); damsIter.hasNext();) {
				
				String UOIID = damsIter.next();
				String assetName = pairs.get(UOIID);
				
				String grabSQL = "select a.RenditionID, a.RenditionNumber, b.FileName, c.Path " +
						"from MediaRenditions a, MediaFiles b, MediaPaths c " +
						"WHERE b.FileName = '" + pairs.get(UOIID) + "' " +
								"AND a.PrimaryFileID = b.FileID " +
								"AND b.PathID = c.PathID";
				
				ResultSet damsRS = DataProvider.executeSelect(tmsConn, grabSQL);
				if(damsRS.next()) {
					//insert mapping into TDIS
					String insertSQL = "insert into TDIS(RenditionID, RenditionNumber, UOIID, OriginalFilePath, OriginalFileName)values(" +
							damsRS.getString(1) + ", '" + damsRS.getString(2) + "', '" + UOIID + "', '" + damsRS.getString(4) + "', '" +
							damsRS.getString(3) + "')";
					
					DataProvider.executeInsert(tmsConn, insertSQL);
				}
				
				//check the color checkbox in dams
				
			}
			
			
			
			
		}
		catch(SQLException sqlex) {
			sqlex.printStackTrace();
		}
		// TODO Auto-generated method stub
		return null;
	}

	private boolean markAsDeleted(Connection tmsConn, String path, String fileName) {
		
		String query = "update TDIS set Deleted = 'Yes' " +
				"WHERE OriginalFilePath = ? AND OriginalFileName = ?";
		
		PreparedStatement stmt;
		try {
			stmt = tmsConn.prepareStatement(query);
			stmt.setString(1, path);
			stmt.setString(2, fileName);
			
			int rowCount = DataProvider.executeUpdate(tmsConn, stmt);
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error marking file {0}as deleted. Rolling back...", fileName);
			}
			else {
				_log.log(Level.ALL, "File {0} successfully marked as deleted.", fileName);
				return true;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return false;
	}

	private HashMap<String, String> getPurgeable(Connection tmsConn, String IDSPathId) {
		
		HashMap<String, String> retval = new HashMap<String, String>();
		String query = "select OriginalFilePath, OriginalFileName " +
				"from TDIS INNER JOIN MediaRenditions on TDIS.RenditionID = MediaRenditions.RenditionID " +
				"INNER JOIN MediaFiles on MediaRenditions.PrimaryFileID = MediaFiles.FileID " +
				"where PathID = " + IDSPathId + " AND Deleted = 'No'";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, query);
		
		try {
			while(rs.next()) {
				retval.put(rs.getString(1), rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "There was an error in the getPurgeable function: {0}", e.getMessage());
			return null;
		}
		
		
		return retval;
	}

	private ArrayList<String> renditionsForIDSSync(Connection tmsConn) {

		ArrayList<String> renditionList = new ArrayList<String>();
		
		String query = "select a.UOIID from TDIS a, MediaRenditions b, MediaFiles c where " +
				"a.RenditionID = b.RenditionID AND " +
				"b.PrimaryFileID = c.FileID AND c.PathID != ? AND " +
				"a.UOIID != '-1'";
		
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			stmt.setInt(1, Integer.parseInt(properties.get("IDSPathId").toString()));
			
			ResultSet rs = DataProvider.executeSelect(tmsConn, stmt);
			while(rs.next()) {
				renditionList.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return renditionList;
	}

	private ArrayList<String> retrievePrimaryFileChanges(Connection tmsConn) {
		
		ArrayList<String> primaryFileChanges = new ArrayList<String>();
		
		String query = "select RenditionID from TDIS where RenditionID in " +
				"(select RenditionID from MediaRenditions where PrimaryFileID in " +
				"(select FileID from MediaFiles where EnteredDate > ?)) and UOIID != '-1'";
		
		//System.out.println("Query: " + query);
		
		
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			Timestamp lastRan = getLastRanTime(tmsConn, "sync");
			java.sql.Date lastRanDate = new java.sql.Date(lastRan.getTime());
			//System.out.println("Last ran date: " + lastRanDate.toGMTString());
			stmt.setDate(1, lastRanDate);
			
			ResultSet rs = DataProvider.executeSelect(tmsConn, stmt);
			while(rs.next()) {
				primaryFileChanges.add(rs.getString(1));
				//System.out.println("retrievePrimaryFileChanges Adding: " + rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "SQLException caught in retrievePrimaryFileChanges: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
		}
		
		return primaryFileChanges;
	}

	private Logger initializeLog() throws SecurityException, IOException {
		DateFormat df = new SimpleDateFormat("MM-dd-yyyy");
		FileHandler fh = new FileHandler("TDIS-" + properties.getProperty("operationType") + ".log." + df.format(new Date()));
		fh.setFormatter(new SimpleFormatter());
		_log.addHandler(fh);
		
		return _log;
		
	}

	private boolean unsyncRendition(String renditionID, Connection tmsConn,
			Connection damsConn) {
		//retrieve UOIID from TDIS
		String query = "select UOIID, OriginalFileName from TDIS where RenditionID = ?";
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			stmt.setInt(1, Integer.parseInt(renditionID));
			ResultSet rs = DataProvider.executeSelect(tmsConn, stmt);
			
			if(rs.next()) {
				String UOIID = rs.getString(1);
				String fileName = rs.getString(2);
				if(!rs.getString(1).equals("-1")) {
					//sync has been made, DAMS changes can be made using UOIID
					query = "update UOIS set CONTENT_STATE = 'DELETED', METADATA_STATE = 'LOCKED', " +
							"CONTENT_STATE_USER_ID = ?, CONTENT_STATE_DT = ?," +
							"METADATA_STATE_USER_ID = ?, METADATA_STATE_DT = ? " +
							"where UOI_ID = ?";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setInt(1, 1);
					stmt.setDate(2, new java.sql.Date(new Date().getTime()));
					stmt.setInt(3, 1);
					stmt.setDate(4, new java.sql.Date(new Date().getTime()));
					stmt.setString(5, UOIID);
				}
				else {
					// UOIID link not present, use filename
					//grab UOIID of new asset
					query = "select UOI_ID from UOIS where NAME = ? and UOI_ID in " +
							"(select UOI_ID from SI_ASSET_METADATA where SOURCE_SYSTEM_ID = 'TMS IMPORT')";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setString(1, fileName);
					_log.log(Level.ALL, "Filename: {0}", fileName);
					ResultSet rs2 = DataProvider.executeSelect(damsConn, stmt);
					rs2.next();
					UOIID = rs2.getString(1);
					
					// if SOURCE_SYSTEM_ID is not 'TMS IMPORT', we have a problem
					query = "update UOIS set CONTENT_STATE = 'DELETED', METADATA_STATE = 'LOCKED', " +
							"CONTENT_STATE_USER_ID = ?, CONTENT_STATE_DT = ?," +
							"METADATA_STATE_USER_ID = ?, METADATA_STATE_DT = ? " +
							"where UOI_ID = ?";
					
					stmt = damsConn.prepareStatement(query);
					stmt.setInt(1, 1);
					stmt.setDate(2, new java.sql.Date(new Date().getTime()));
					stmt.setInt(3, 1);
					stmt.setDate(4, new java.sql.Date(new Date().getTime()));
					stmt.setString(5, UOIID);
				}
				int result = DataProvider.executeUpdate(damsConn, stmt);
				
				if(result == 1) {
					//update was successful, clear TDIS record
					query = "delete from TDIS where UOIID = ?";
					stmt = tmsConn.prepareStatement(query);
					stmt.setString(1, rs.getString(1));
					
					DataProvider.executeUpdate(tmsConn, stmt);
					
					_log.log(Level.ALL, "Asset {0} has been successfully unsynced.", fileName);
					
				}
				else {
					_log.log(Level.ALL, "There was an error updating the DAMS UOIS table. Leaving TMS Sync record intact for debugging.");
					_log.log(Level.ALL, "Query: {0}", query);
				}
			}
			else {
				//no sync record present, nothing to do
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "SQLException in unsyncRendition: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
		return false;
	}

	private ArrayList<String> retrieveUnsyncedRenditions(Connection tmsConn) {
		ArrayList<String> renditionIDs = new ArrayList<String>();
		//retrieve unsynced renditions - renditions with IsColor = 0, and there is a sync record for them
		String query = "select RenditionID from MediaRenditions where IsColor = 0 and RenditionID in (select RenditionID from TDIS)";
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			
			ResultSet rs = DataProvider.executeSelect(tmsConn, query);
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
				_log.log(Level.ALL, "Unsyncing rendition {0} in retrieveUnsyncedRenditions. Query: {1}", new Object[]{rs.getString(1), query});
				//System.out.println("retrieveUnsyncedRenditions Adding: " + rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in retrieveUnsyncedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
		return renditionIDs;
	}

	private boolean syncFilePath(Connection tmsConn,
			String UOIID, String UAN) {
		
		//grab original filename and file path
		String fileName = null;
		String filePath = null;
		String renditionID = null;
		/*String query = "select mf.FileName, mp.Path from MediaFiles mf, MediaPaths mp " +
				"where mf.PathID = mp.PathID and " +
				"mf.RenditionID = (select RenditionID from MediaRenditions where RenditionNumber = '" + 
				tempRendition.getRenditionNumber() + "')"; */
		
		String query = "select mf.FileName, mp.Path, mf.RenditionID from MediaFiles mf, MediaPaths mp " +
				"where mf.PathID = mp.PathID and " +
				"mf.RenditionID = (select RenditionID from TDIS where UOIID = '" + UOIID + "')"; 
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next()) {
				fileName = rs.getString(1);
				filePath = rs.getString(2);
				renditionID = rs.getString(3);
			}
			
			//update TDIS
			/*
			query = "update TDIS set OriginalFilePath = '" + filePath + "', OriginalFileName = '" + fileName + "' where UOIID = '" 
					+ UOIID + "'";
			
			stmt = tmsConn.prepareStatement(query);
			
			int rowCount = stmt.executeUpdate();
			
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error updating the TDIS table.");
				return false;
			}
			*/
			//update MediaFiles
			query = "update MediaFiles set PathID = " + properties.getProperty("IDSPathId") + ", FileName = '" + UAN
					+ "' WHERE FileID = (select PrimaryFileID from MediaRenditions where RenditionID = " + renditionID + ")";
			
			stmt = tmsConn.prepareStatement(query);
			
			int rowCount = stmt.executeUpdate();
			
			if(rowCount != 1) {
				_log.log(Level.ALL, "There was an error updating the TDIS table.");
				return false;
			}
			
			return true;
		
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in syncFilePath: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//sqlex.printStackTrace();
		}
				
		return false;
	}

	/*private String eligibleForSync(Connection damsConn, Connection tmsConn, String UOIID) {
		
		String query = "select UOI_ID, UAN from SI_IDS_EXPORT where UOI_ID = '" + UOIID + "' AND EXPORT_DATE > ?";
		Timestamp lastRan = getLastRanTime(tmsConn, "sync");
		java.sql.Date lastRanDate = new java.sql.Date(lastRan.getTime());
		String UAN = null;
		
		try {
			PreparedStatement stmt = damsConn.prepareStatement(query);
			stmt.setDate(1, lastRanDate);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				UAN = rs.getString(1);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in eligibleForSync: " + e.getMessage());
			_log.log(Level.ALL, "Query: " + query);
			//e.printStackTrace();
		}
		
		return UAN;
	}*/
	
	private HashMap<String, String> getUANPairs(Connection damsConn, Connection tmsConn, ArrayList<String> UOIIDS) {
		
		String query = "select UOI_ID, UAN from SI_IDS_EXPORT where UOI_ID in (";
		for(Iterator<String> iter = UOIIDS.iterator(); iter.hasNext();) {
			query += "'" + iter.next() + "'";
			if(iter.hasNext()) {
				query += ",";
			}
		}
		query += ") AND EXPORT_DATE > ?";
		Timestamp lastRan = getLastRanTime(tmsConn, "sync");
		java.sql.Date lastRanDate = new java.sql.Date(lastRan.getTime());
		HashMap<String, String> retval = new HashMap<String, String>();
		
		try {
			PreparedStatement stmt = damsConn.prepareStatement(query);
			stmt.setDate(1, lastRanDate);
			ResultSet rs = stmt.executeQuery();
			if(rs.next()) {
				retval.put(rs.getString(1), rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in eligibleForSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
		
		return retval;
	}

	private boolean requiresIDSSync(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		String query = "select PathID from MediaFiles where FileID = (select PrimaryFileID from MediaRenditions where RenditionID = "
				+ tempRendition.getRenditionID() + ")";
		
		PreparedStatement stmt;
		String pathID = null;
		try {
			stmt = tmsConn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next()) {
				pathID = rs.getString(1);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in requiresIDSSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
		
		if(pathID != null && pathID.equals(properties.get("IDSPathId"))) {
			return false;
		}
		else {
			return true;
		}
	}

	private ArrayList<String> getRenditionsForFileNames(Connection tmsConn,
			HashMap<String, String> syncPairs) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		//create the query
		String sql = "select RenditionID from TDIS where UOIID in (";
		
		for(Iterator<String> iter = syncPairs.keySet().iterator(); iter.hasNext();) {
			String UOIID = syncPairs.get(iter.next());
			sql += "'" + UOIID + "'";
			if(iter.hasNext()) {
				sql += ", ";
			}
		}
		sql += ")";
		////system.out.println("QUERY: " + sql);
		ResultSet rs;
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(sql);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in getRenditionsForFileNames: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
		
		return renditionIDs;
	}

	private ArrayList<String> getRecentlyFlaggedRenditions(
			Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		//create the query
		String sql = "select RenditionID from MediaRenditions where IsColor = 1 and RenditionID not in (select RenditionID from TDIS)";
		
		ResultSet rs;
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(sql);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in getRecentlyFlaggedRenditions: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
		
		return renditionIDs;
	}

	private boolean syncNewRecords(Connection tmsConn, Connection damsConn,
			HashMap<String, String> syncPairs) {
		
		String query = "update TDIS set UOIID = ? where OriginalFileName = ?";
		PreparedStatement stmt = null;
		ResultSet rs = null;
		//System.out.println("syncpairs: " + syncPairs.keySet().size());
		for(Iterator<String> iter = syncPairs.keySet().iterator();iter.hasNext();) {
			String key = iter.next();
			//System.out.println("Key: " + key);
			try {
				stmt = tmsConn.prepareStatement(query);
				
				//syncPairs = (fileName, UOIID)
				//check if syncPairs has a filename with same name but with .tif
				String tifFilename = key.replace(".jpg", ".tif");
				//System.out.println("TIF: " + tifFilename);
				if(syncPairs.keySet().contains(tifFilename) && !key.equals(tifFilename)) {
					//tif file was ingested alongside, make the metadata sync with that UOIID
					stmt.setString(1, (String)syncPairs.get(tifFilename));
					//syncPairs.remove(tifFilename);
					//syncPairs.remove(key);
					//boolean linkResult = createLink(damsConn, syncPairs.get(tifFilename).toString(), key);
				}
				else {
					stmt.setString(1, syncPairs.get(key));
					//syncPairs.remove(key);
				}
				
				stmt.setString(2, key);
				//if so, setString with that UOIID, rather than key
				//flag for creating the link
				
				
				//System.out.println("Syncing file " + key + " with UOIID " + syncPairs.get(key).toString());
				
				int result = stmt.executeUpdate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				_log.log(Level.ALL, "SQLException throwin in syncNewRecords, trying to sync file {0}. Skipping...", key);
				_log.log(Level.ALL, "Message: {0}", e.getMessage());
				
			}
		}
		
		
		return false;
	}

	/*private boolean createLink(Connection damsConn, String parentUOIID, String childUOIID) {
		// TODO Auto-generated method stub
		boolean retval = false;
		
		String query = "insert into TEAMS_LINKS(SRC_TYPE, SRC_VALUE, DEST_TYPE, DEST_VALUE, LINK_TYPE)values" +
				"('UOI', ?, 'UOI', ?, 'PARENT'"
		
		return retval;
		
	}*/

	private HashMap<String, String> retrieveUOIIDSForSync(Connection damsConn,
			ArrayList<String> fileNames) {
		
		HashMap<String, String> pairings = new HashMap<String, String>();
		String query = new String();
		
		Statement stmt = null;
		ResultSet rs = null;
		
		for(Iterator<String> iter = fileNames.iterator(); iter.hasNext();) {
			String fileName = iter.next();
			query = "select a.NAME, a.UOI_ID from UOIS a, SI_ASSET_METADATA b " +
					"where NAME like '" + FilenameUtils.removeExtension(fileName) + "%' " +
					"and b.SOURCE_SYSTEM_ID = 'TMS IMPORT'" +
					"and a.UOI_ID = b.UOI_ID " +
					"and a.CONTENT_STATE != 'DELETED'";
			
			try {
				stmt = damsConn.createStatement();
				rs = stmt.executeQuery(query);
				
				while(rs.next()) {
					pairings.put(rs.getString(1), rs.getString(2));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				_log.log(Level.ALL, "SQLException thrown while retrieving UOI_ID for file {0}. Skipping...", fileName);
				_log.log(Level.ALL, "Message: {0}", e.getMessage());
			}
			
		}
		
		return pairings;
	}

	private ArrayList<String> retrieveRenditionsPendingSync(Connection tmsConn) {
		
		ArrayList<String> fileNames = new ArrayList<String>();
		String query = "select OriginalFileName from TDIS where UOIID = '-1'";
		
		Statement stmt;
		try {
			stmt = tmsConn.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()) {
				fileNames.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			_log.log(Level.ALL, "Exception in retrieveRenditionsPendingSync: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
		}
		
		return fileNames;
	}

	private String getUOIIDForRendition(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		String UOIID = null;
		String query = "select UOIID from TDIS where RenditionID = '" + tempRendition.getRenditionID() + "'";
		
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			
			if(rs.next()) {
				UOIID = rs.getString(1);
			}
			else {
				_log.log(Level.ALL, "Rendition {0} is not synced between TMS and DAMS. Skipping...", tempRendition.getRenditionNumber());
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			_log.log(Level.ALL, "Exception in getUOIIDForRendition: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", query);
			//e.printStackTrace();
		}
		
		
		return UOIID;
	}

	private boolean updateMetadataManually(Connection damsConn,
			TMSMediaRendition tempRendition, String UOIID) {
		
		DateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String dateString = df1.format(new Date());
		
		//create query - UOIS table
		StringBuffer query = new StringBuffer();
		query.append("update UOIS set ");
		query.append("NAME = '" + String.valueOf(tempRendition.getFileName()) + "', ");
		query.append("METADATA_STATE_DT = TO_DATE('" + dateString + "', 'MM/DD/YYYY') ");
		query.append("where UOI_ID = '" + UOIID + "'");
		
		String queryUOIS = query.toString();
		queryUOIS = queryUOIS.replace('&', '+');
		_log.log(Level.ALL, "UOIS query: {0}", queryUOIS);
		
		//create query - SI_ASSET_METADATA table
		String queryMetadata = tempRendition.getMetadataQuery(UOIID);
		_log.log(Level.ALL, "Metadata query: {0}", queryMetadata);
		
		PreparedStatement stmt = null;
		int result = DataProvider.executeUpdate(damsConn, queryUOIS);
		result = DataProvider.executeUpdate(damsConn, queryMetadata);
		
		//if(result == 1) {
			return true;
		//}
		//else {
			//return false;
		//}
		
		
	}

	private boolean placeInHotFolder(File hotFolder,
			TMSMediaRendition tempRendition, XMLBuilder xml, File workFolder) {
		
		String fileName = tempRendition.getFileName().split("\\\\")[tempRendition.getFileName().split("\\\\").length-1];
		
		// Create the metadata xml file into the work folder
		File xmlFile = new File(workFolder.getAbsolutePath() + "/" + tempRendition.getName()+ ".xml");
		Properties outputProperties = new Properties();
		outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
		
	    try
	    {
	    	FileWriter xmlWriter = new FileWriter(xmlFile);
	    	xml.toWriter(xmlWriter, outputProperties);
	    	//FileUtils.writeStringToFile(xmlFile, xml);
	    	
	    	//move asset file to workfolder
	    	Scanner scanner;
		    File assetFile = new File(convertMediaPath(tempRendition.getStructuralPath()) + "/" + tempRendition.getFileName());
		    //System.out.println("Copying from " + assetFile.getAbsolutePath());
		    File destFile = new File(workFolder.getAbsolutePath() + "/" + fileName);
		    _log.log(Level.ALL, "Beginning file copy to work folder...");
		    _log.log(Level.ALL, "Source file size: {0}", assetFile.length());
		    boolean isCopying = true;
		    FileUtils.copyFile(assetFile, destFile);
		    /*while(isCopying) {
		    	try {
	                scanner = new Scanner(destFile);
	                isCopying = false;
	            } catch (FileNotFoundException e) {
	                System.out.println("File not found or is in copy State. ");
	                try {
						Thread.sleep(100);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
	            }
		    }*/
		    _log.log(Level.ALL, "File copy to work folder complete.");
		    _log.log(Level.ALL, "Beginning file copy to hot folder...");
		    //if successful, copy both files to hotfolder
		    FileUtils.copyFile(destFile, new File(hotFolder.getAbsolutePath() + "/" + "MASTER" + "/" + fileName));
		    FileUtils.copyFile(xmlFile, new File(hotFolder.getAbsolutePath() + "/" + "METADATA" + "/" + fileName + ".xml"));
		    
		    _log.log(Level.ALL, "File copy to hot folder complete.");
		    
		    return true;
	    }
	    catch (IOException ioe)
	    {
	    	//ioe.printStackTrace();
	      _log.log(Level.ALL, "FileUtils threw IOException while attempting to move {0} under {1}.\nIOException message: {2}", new Object[]{tempRendition.getFileName(), xmlFile.getAbsolutePath(), ioe.getMessage()});
	      	return false;
	    } catch (TransformerException e) {
			// TODO Auto-generated catch block
	    	_log.log(Level.ALL, "TransformerException in placeInHotFolder: {0}", e.getMessage());
			return false;
		}
	    
	}

	/*private boolean syncIfNeeded(Connection tmsConn, Connection damsConn,
			String renditionNumber) {
		
		boolean retval = false;
				
		String sql = "select SI_ASSET_METADATA.title, SI_ASSET_METADATA.UOI_ID, SI_IDS_EXPORT.UAN from " +
				"SI_ASSET_METADATA INNER JOIN SI_IDS_EXPORT on SI_ASSET_METADATA.UOI_ID = SI_IDS_EXPORT.UOI_ID " +
				"where SI_IDS_EXPORT.UAN is not null and SI_ASSET_METADATA.title = ?";
		
		////system.out.println("SQL: " + sql);
		
		try {
			PreparedStatement testStatement = damsConn.prepareStatement(sql);
			
			testStatement.setString(1, renditionNumber);
			
			ResultSet rs = DataProvider.executeSelect(damsConn, testStatement);
			
			if(rs.next()) {
				//grab uoi and UAN
				String uoi = rs.getString(2);
				String uan = rs.getString(3);
				
				//grab original filename and file path
				sql = "select mf.FileName, mp.Path from MediaFiles mf, MediaPaths mp " +
						"where mf.PathID = mp.PathID and " +
						"mf.RenditionID = (select RenditionID from MediaRenditions where RenditionNumber = '" + 
						renditionNumber + "')";
				
				////system.out.println("Sql2: " + sql);
				
				ResultSet rs2 = DataProvider.executeSelect(tmsConn, sql);
				
				if(rs2.next()) {
					String fileName = rs2.getString(1);
					String path = rs2.getString(2);
					String checkSum = new String();
					
					//get checksum for file
					File tempFile = new File(path + File.separator + fileName);
					//system.out.println("File: " + path + File.separator + fileName);
					
					try {
						checkSum = ChecksumUtils.getMD5ChecksumAsString(tempFile);
					}
					catch(ChecksumException cse) {
						cse.printStackTrace();
						throw new SQLException("There was an error creating the checksum for file " + path + File.separator + fileName);
					}
					
					//place the filename, path, UOIID, and checksum inside the TDIS table
					sql = "update TDIS set OriginalFilePath = '" + path + "', OriginalFileName = '" + fileName + "', UOIID = '" + uoi + "', " +
							"Checksum =  '" + checkSum + "' " +
							"where RenditionNumber = '" + renditionNumber + "'";
					
					int success = DataProvider.executeUpdate(tmsConn, sql);
					
					if(success == 0) {
						throw new SQLException("There was an error updating the TMS TDIS record.");
					}
					
					String idsPath = properties.getProperty("IDSPathId");
					
					//place the new path and filename into the MediaFiles record
					sql = "update MediaFiles set PathID = " + idsPath + ", FileName = '" + uan + "' where " +
							"RenditionID = (select RenditionID from MediaRenditions where RenditionNumber = '" + 
							renditionNumber + "')"; 
					
					success = DataProvider.executeUpdate(tmsConn, sql);
					
					if(success == 0) {
						throw new SQLException("There was an error updating the TMS MediaFiles record.");
					}
					
					_log.log(Level.ALL, "Rendition " + renditionNumber + " successfully updated.");
					
					return true;
				}
				else {
					throw new SQLException("No asset record found in TMS with title " + renditionNumber);
				}
			}
			else {
				throw new SQLException("No asset record found in DAMS with title " + renditionNumber);
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Error occurred while attempting to sync " + renditionNumber + ".");
			_log.log(Level.ALL, "Reason: " + sqlex.getMessage());
			//sqlex.printStackTrace();
			return false;
		}
	}*/

	private String convertMediaPath(String structuralPath) {
		
		//get the mediaDrive path specified in the config file
		String mediaDrive = this.properties.getProperty("mediaDrive");
		String driveLetter = mediaDrive.split(":")[0];
		String uncPath = mediaDrive.split(":")[1];
		
		if(structuralPath.startsWith(driveLetter)) {
			//replace the mapped drive path with the unc path
			return structuralPath.replace(driveLetter + ":", uncPath);
		}
		else {
			return structuralPath;
		}
	}

	private static ArrayList<String> getRenditionsForUAN(Connection tmsConn) throws SQLException {
		
		ArrayList<String> retval = new ArrayList<String>();
		String sql = "select RenditionNumber from TDIS where UOIID = '-1'";
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		
		while(rs.next()) {
			String tempRendition = rs.getString(1);
			retval.add(tempRendition);
		}
		
		return retval;
	}

	private boolean createIngestRecord(Connection tmsConn,
			TMSMediaRendition tempRendition) {
		
		//check to make sure the rendition doesn't already exist in the TDIS
		String sql = "select count(*) from TDIS where RenditionID = " + tempRendition.getRenditionID();
		
		ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		try {
			rs.next();
			if(rs.getInt(1) != 0) {
				_log.log(Level.ALL, "Rendition {0} already synced. Skipping...", tempRendition.getRenditionNumber());
				return true;
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "There was an error retrieving the sync data for rendition {0}. Skipping...", tempRendition.getRenditionNumber());
			return false;
		}
		
		String filePath = tempRendition.getStructuralPath();
		String fileName = new String();
		String[] tempStrings = tempRendition.getFileName().split("\\\\");
		
		for(int i = 0; i < tempStrings.length; i++) {
			if(i == tempStrings.length-1) {
				fileName = tempStrings[i].split("/")[tempStrings[i].split("/").length-1];
			}
			else {
				filePath = filePath.concat(File.separator);
				filePath = filePath.concat(tempStrings[i]);
			}
		}
		
		//insert mapping into TDIS
		sql = "insert into TDIS(RenditionID, RenditionNumber, UOIID, OriginalFilePath, OriginalFileName)values(" +
				tempRendition.getRenditionID() + ", '" + tempRendition.getRenditionNumber() + "', -1, '" + filePath + "', '" +
				fileName + "')";
		
		boolean success = false;
		
		success = DataProvider.executeInsert(tmsConn, sql);
		
		return success;
		
	}
	
	private boolean createLogEntry(Connection tmsConn, String operationType) {
		
		//insert entry into TDIS_Log
		String sql = "insert into TDIS_Log(OperationType, LastRan)values('" + operationType + "', CURRENT_TIMESTAMP)";
				
		boolean success = DataProvider.executeInsert(tmsConn, sql);
				
		return success;
				
	}

	private File buildBag(Logger logger, Connection conn, File workFolder, String bagname, TMSMediaRendition asset, HoleyBagData item, XMLBuilder xml) throws BagException
	  {
	    // Create folder structure of the bag:
	    File bagFolder = Bagger.makeFolderStructure(logger, workFolder, bagname);
	    
	    // Create the metadata xml file into the data folder
	    File xmlFile = new File(bagFolder.getAbsolutePath() + File.separator+"data"+File.separator + item.xmlFilename);
	    Properties outputProperties = new Properties();
		outputProperties.put(javax.xml.transform.OutputKeys.INDENT, "yes");
	    try
	    {
	    	FileWriter xmlWriter = new FileWriter(xmlFile);
	    	xml.toWriter(xmlWriter, outputProperties);
	    	//FileUtils.writeStringToFile(xmlFile, xml);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("FileUtils.writeStringToFile threw IOException while attempting to write to "
	          +item.xmlFilename+" under "+xmlFile.getAbsolutePath()
	          +".\nIOException message: "+ioe.getMessage());
	    } catch (TransformerException e) {
			// TODO Auto-generated catch block
	    	_log.log(Level.ALL, "TransformerException in buildBag: {0}", e.getMessage());
			//e.printStackTrace();
		}
	    
	    String mdChecksum = "";
	    try
	    {
	      mdChecksum = ChecksumUtils.getMD5ChecksumAsString(xmlFile);
	    }
	    catch (ChecksumException e)
	    {
	      throw new BagException("ChecksumException while attempting to get checksum for "+xmlFile.getAbsolutePath());
	    }
	    
	    HashMap<String,String> manifestData = new HashMap<String, String>();
	    manifestData.put(item.checksum, "data"+File.separator+item.filename);
	    manifestData.put(mdChecksum, "data"+File.separator+item.xmlFilename);
	    try
	    {
	      Bagger.writeManifest(logger, bagFolder, manifestData);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing manifest file!\nIOException message: "+ioe.getMessage());
	    }
	    
	    try
	    {
	      Bagger.writeFetchTxt(logger, bagFolder, item.url, "data/"+item.filename);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing fetch.txt!\nIOException message: "+ioe.getMessage());
	    }
	    
	    try
	    {
	      Bagger.writeBagitFile(logger, bagFolder);
	    }
	    catch (IOException ioe)
	    {
	      throw new BagException("IOException while writing bagit file!\nIOException message: "+ioe.getMessage());
	    }
	    
	    return bagFolder;
	  }

	/**
	 * @param Connection tmsConn - connection to the TMS database
	 * @param ArrayList<String> newRenditions - the rendition numbers of the new renditions
	 * 											to be loaded.
	 * @return ArrayList of populated TMSMediaRendition objects. null if an error occurs.
	 * 
	 * This function will pull data for the MediaRendition RenditionNumbers found in the 
	 * supplied ArrayList, and will populate TMSMediaRendition objects with
	 */
	private ArrayList<TMSMediaRendition> loadTMSData(Connection tmsConn, ArrayList<String> newRenditions, String action) {
		
		ArrayList<TMSMediaRendition> renditionObjects = new ArrayList<TMSMediaRendition>();
		TMSMediaRenditionFactory renditionFactory = new TMSMediaRenditionFactory();
		TMSMediaRendition tempRendition = new TMSMediaRendition();
		
		for(Iterator<String> iter = newRenditions.iterator(); iter.hasNext();) {
			tempRendition = renditionFactory.retrieveRendition(iter.next(), properties, tmsConn, _log);
			if(tempRendition != null) {
				////system.out.println("Retrieved asset: " + tempRendition);
				tempRendition.setAction(action);
				renditionObjects.add(tempRendition);
			}
			else {
				_log.log(Level.ALL, "Error retrieving asset. Skipping...");
			}
			
		}
		
		
		return renditionObjects;
	}

	/**
	 * @param tmsConn
	 * @return List of media rendition IDs for newly created renditions.
	 * 
	 * This method will pull the timestamp of the last time the program was run from the TDIS_LOG table.
	 * It will then find the MediaRendition records which have an EnteredDate > the TDIS_LOG timestamp.
	 * It will then return all the MediaRendition RenditionIDs in an ArrayList.
	 */
	private ArrayList<String> retrieveNewRenditions(Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		Timestamp lastRan = getLastRanTime(tmsConn, "ingest");
		
		//create the query
		String sql = "select RenditionID from MediaRenditions where EnteredDate >= ? AND IsColor = 1 AND PrimaryFileID not in (select FileID from MediaFiles where PathID = 9)";
		
		//System.out.println("sql: " + sql);
		
		ResultSet rs;
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(sql);
			
			stmt.setTimestamp(1, lastRan);
			
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			while(rs.next()) {
				renditionIDs.add(rs.getString(1));
			}
		}
		catch(SQLException sqlex) {
			_log.log(Level.ALL, "Exception in retrieveNewRenditions: {0}", sqlex.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
		
		return renditionIDs;
	}
	
	private ArrayList<String> retrieveUpdatedRenditions(Connection tmsConn) {
		
		ArrayList<String> renditionIDs = new ArrayList<String>();
		
		HashMap<String, ArrayList<String>> auditValues = new HashMap<String, ArrayList<String>>();
		Timestamp lastRan = getLastRanTime(tmsConn, "sync");
		//get the AuditTrail table values
		ResultSet rs;
		String sql = "select TableName, ObjectID from AuditTrail where EnteredDate >= ?";
		
		try {
			PreparedStatement stmt = tmsConn.prepareStatement(sql);
			stmt.setTimestamp(1, lastRan);
			rs = DataProvider.executeSelect(tmsConn, stmt);
			
			//populate the AuditTrail values
			while(rs.next()) {
				String tableName = rs.getString(1);
				String objectID = rs.getString(2);
				////system.out.println("tableName: " + tableName);
				////system.out.println("objectID: " + objectID);
				if(auditValues.get(tableName) == null) {
					ArrayList<String> tempList = new ArrayList<String>();
					tempList.add(objectID);
					auditValues.put(tableName, tempList);
				}
				else {
					ArrayList<String> tempList = auditValues.get(tableName);
					if(!tempList.contains(objectID)) {
						tempList.add(objectID);
					}
					auditValues.put(tableName, tempList);
				}
			}
			
			//run the queries to get the RenditionIDs
			AuditTrailReader auditReader = new AuditTrailReader(this.properties.getProperty("auditConfigFile"));
			
			for(Iterator<?> iter = auditReader.getTableNames().iterator(); iter.hasNext();) {
				String table = iter.next().toString();
				////system.out.println("Table: " + table);
				String values = new String();
				if(auditValues.containsKey(table)) {
					for(Iterator<String> valueIter = auditValues.get(table).iterator();valueIter.hasNext();) {
						values += valueIter.next().toString();
						if(valueIter.hasNext()) {
							values += ",";
						}
					}
					
					sql = auditReader.getQuery(table);
					////system.out.println("SQL before the replace: " + sql);
					sql = sql.replace("?", values);
					////system.out.println("query: " + sql);
					rs = DataProvider.executeSelect(tmsConn, sql);
					
					while(rs.next()) {
						renditionIDs.add(rs.getString(1));
					}
				}
			}
		}
		catch(SQLException e) {
			_log.log(Level.ALL, "Exception in retrieveUpdatedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return null;
		}
		
		return renditionIDs;
	}

	private Timestamp getLastRanTime(Connection tmsConn, String opType) {
		
		Timestamp retval = null;
		
		String sql = "select top 1 LastRan from TDIS_Log where OperationType = '" + opType + "' order by LastRan desc";
		
		try {
			ResultSet rs = DataProvider.executeSelect(tmsConn, sql);
		
			if(rs.next()) {
				retval = rs.getTimestamp(1);
			}
			else {
				//no rows, default to 24 hours prior
				retval = new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000L);
			}
			
		}catch(SQLException e) {
			_log.log(Level.ALL, "Exception in retrieveUpdatedRenditions: {0}", e.getMessage());
			_log.log(Level.ALL, "Query: {0}", sql);
			//sqlex.printStackTrace();
			return new Timestamp(System.currentTimeMillis() - 24 * 60 * 60 * 1000L);
		}
		
		return retval;
	}

	/**
	 * @param configFileName
	 * @throws FileNotFoundException
	 * @throws Exception
	 * 
	 * This method verifies whether the filename passed exists in the file system. Then it 
	 * loads the properties from the file into a Properties class. The file should be formatted as
	 * below:
	 * 		key=value
	 * 
	 * After loading the properties, it then cycles through an array of required properties, and throws
	 * an exception if one is missing from the properties.
	 */
	private void loadProperties(String configFileName) throws FileNotFoundException, Exception {
		
		//verify file exists
		if(configFileName == null) {
			throw new FileNotFoundException();
		}
		
		//load properties from the file
		File configFile = new File(configFileName);
		FileInputStream inStream = new FileInputStream(configFile);
		BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
		
		String lineHolder = new String();
		lineHolder = reader.readLine();
		while(lineHolder != null) {
			if(!lineHolder.startsWith("#") && !lineHolder.trim().equals("") && lineHolder.split("=").length > 1) {
				//properties.setProperty(lineHolder.split("=")[0].trim(), lineHolder.split("=")[1].trim());
				properties.setProperty(lineHolder.split("=")[0].trim(), lineHolder.substring(lineHolder.indexOf("=")+1));
			}
			lineHolder = reader.readLine();
		}
		
		//verify that required fields are present
		for(int i = 0; i < requiredProps.length; i++) {
			String reqProp = requiredProps[i];
			if(!properties.containsKey(reqProp)) {
				throw new Exception(reqProp);
			}
		}
		
		return;
	}
	
	private HoleyBagData getHoleyBagData(TMSMediaRendition rendition, Connection tmsConn) throws ChecksumException {
		HoleyBagData item;
		
		String path = rendition.getStructuralPath() + File.separator + rendition.getFileName();
		
		String fileName = new String();
		if(rendition.getName().contains("\\")) {
			fileName = rendition.getName().split("\\\\")[rendition.getName().split("\\\\").length-1];
		}
		else {
			fileName = rendition.getName();
		}
		
		File tempFile = new File(path);
		
		String checksum = new String();
		
		if(rendition.isSynced(tmsConn, _log)) {
			//grab the checksum from the TDIS table
			checksum = rendition.getChecksum(tmsConn, _log);	
		}
		else { 
			checksum = ChecksumUtils.getMD5ChecksumAsString(tempFile);
		}
		
		item = new HoleyBagData(0, path, fileName, checksum, path, fileName + ".xml");
		
		
		return item;
	}
	
	public boolean sendIngestEmail(ArrayList<String> successAssets, ArrayList<String> failedAssets, ArrayList<String> ingestedFromDAMS) {
		
		//get server, to, and from 
		if(!properties.containsKey("emailServer") ||
				!properties.containsKey("emailTo") ||
				!properties.containsKey("emailFrom")) {
			
			_log.log(Level.ALL, "One or more email parameters are absent from the config file. Skipping email message...");
			return false;
		}
		
		String server = (String)properties.get("emailServer");
		String emailTo = (String)properties.get("emailTo");
		String emailFrom = (String)properties.get("emailFrom");
		
		Date testDate = new Date();
		SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String subject = "TMS/DAMS Integration Ingest Report for " + df1.format(testDate);
		
		StringBuffer bodyBuffer = new StringBuffer();
		
		bodyBuffer.append("TMS/DAMS Integration Ingest Report for " + df1.format(testDate));
		bodyBuffer.append("<br/><br/>");
		
		if(!successAssets.isEmpty()) {
			bodyBuffer.append("The following assets were ingested successfully from TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = successAssets.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!ingestedFromDAMS.isEmpty()) {
			bodyBuffer.append("The following DAMS assets were created successfully in TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = ingestedFromDAMS.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!failedAssets.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the ingest process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedAssets.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		bodyBuffer.append("<br/><br/>");
		bodyBuffer.append("Please contact the DAMS Team with any issues related to TMS/DAMS Integration.");
		bodyBuffer.append("<br/>");
		
		try {
			sendEmail(server, emailFrom, emailTo, subject, bodyBuffer.toString(), new String(), new String());
			return true;
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	public boolean sendSyncEmail(ArrayList<String> metadataAssets, ArrayList<String> IDSAssets, ArrayList<String> failedMetadata, ArrayList<String> failedIDS, 
			ArrayList<String> unsyncedRenditions) {
		
		//get server, to, and from 
		if(!properties.containsKey("emailServer") ||
				!properties.containsKey("emailTo") ||
				!properties.containsKey("emailFrom")) {
			
			_log.log(Level.ALL, "One or more email parameters are absent from the config file. Skipping email message...");
			return false;
		}
		
		String server = (String)properties.get("emailServer");
		String emailTo = (String)properties.get("emailTo");
		String emailFrom = (String)properties.get("emailFrom");
		
		Date testDate = new Date();
		SimpleDateFormat df1 = new SimpleDateFormat("MM/dd/yyyy");
		String subject = "TMS/DAMS Metadata Sync Report for " + df1.format(testDate);
		
		StringBuffer bodyBuffer = new StringBuffer();
		
		bodyBuffer.append("TMS/DAMS Metadata Sync Report for " + df1.format(testDate));
		bodyBuffer.append("<br/><br/>");
		
		if(!metadataAssets.isEmpty()) {
			bodyBuffer.append("The following assets had metadata changes synced from TMS: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = metadataAssets.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!IDSAssets.isEmpty()) {
			bodyBuffer.append("The following assets are now referencing their IDS derivative files: ");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = IDSAssets.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		
		if(!unsyncedRenditions.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets were unsynced and deleted from the DAMS:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = unsyncedRenditions.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!failedMetadata.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the metadata sync process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedMetadata.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
		
		if(!failedIDS.isEmpty()) {
			bodyBuffer.append("<br/><br/>");
			bodyBuffer.append("The following assets experienced errors during the IDS sync process, and were skipped as a result:");
			bodyBuffer.append("<br/><hr/><br/>");
			
			for(Iterator<String> iter = failedIDS.iterator(); iter.hasNext();) {
				bodyBuffer.append(iter.next());
				bodyBuffer.append("<br/>");
			}
		}
	
		
		bodyBuffer.append("<br/><br/>");
		bodyBuffer.append("Please contact the DAMS Team with any issues related to TMS/DAMS Integration.");
		bodyBuffer.append("<br/>");
		
		try {
			sendEmail(server, emailFrom, emailTo, subject, bodyBuffer.toString(), new String(), new String());
			return true;
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	
	public void sendEmail( String emailSrv, String fromAddr, String toEmailAddr, String subject, 
			String body, String emailBodyTop, String emailBodyBottom)
	throws MessagingException {

		Properties mailProps = new Properties();
		mailProps.put("mail.smtp.host", emailSrv);      
		Session session = Session.getDefaultInstance( mailProps, null );

		MimeMessage message = new MimeMessage( session );
		message.setFrom(new InternetAddress(fromAddr));
		String[] toEmailAddrArray = toEmailAddr.split(",");
		for (int i = 0; i < toEmailAddrArray.length; i++) {
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(toEmailAddrArray[i]));
		}
		message.setSubject(subject);         

		// create the Multipart and its parts to it
		Multipart parts = new MimeMultipart();

		// create and fill the Body
		String emailContent = emailBodyTop +"<br>"+body+"<br>"+emailBodyBottom;
		MimeBodyPart bodyPart = new MimeBodyPart();
		bodyPart.setContent(emailContent,"text/html");
		parts.addBodyPart(bodyPart);

		// add the Multipart to the message
		message.setContent(parts);
		Transport.send(message);       
	}
	
	
}

class HoleyBagData
{
  public int id;
  public String rrpath;
  public String filename;
  public String checksum;
  public String url;
  public String xmlFilename;
  public HoleyBagData(int id, String rrpath, String filename, String checksum,
      String url, String xmlFilename)
  {
    super();
    this.id = id;
    this.rrpath = rrpath;
    this.filename = filename;
    this.checksum = checksum;
    this.url = url;
    this.xmlFilename = xmlFilename;
  }
}
