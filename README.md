# cdis
Collections - DAMS Integration System

CDIS Overview:
CDIS is a suite of batch process that integrates the Digital Asset Management System (DAMS) with the SI unit's Collection Information System (CIS).
The batch processes are run on a scheduler (such as cron) throughout the day, but can also be manually executed.
  
  
The batch process operation types are as follows:

* cisUpdate          (updates select values in the CIS based on values in DAMS).

* createCisMedia   (Adds a brand new CIS media record based on an existing DAMS image and attaches it to an existing object in the CIS).

* linkCisRecord        (associates the CIS record to a DAMS record in the CDIS mapping tables).  

* linkDamsRecord	   (records the DAMS record to a CDIS record in the CDIS mapping tables). 

* metadataSync     (Brings over metadata from the CIS into the DAMS).

* idsSync          (Updates the Cis with a reference to the IDS uan so the CIS no longer needs to retain an independant full-size copy of the image)

* thumbnailSync    (replaces the thumbnail image in the CIS with the current thumbnail image in the DAMS system).

* timeFrameReport  (generates and emails report showing the status/completion/errors of files through CDIS for a given timeframe)

* sendToHotFolder  (Copies media to the appropriate DAMS hotfolder so it can be ingested into DAMS).

* vfcuDirReport   (generates and emails report showing the status/completion/errors of files through CDIS for a given vfcu source directory)

The java code for ALL of these execution types is found in this repository and is to be compiled into a single .jar file.
Each SI unit uses different combinations of the above operation types to fully integrate the DAMS with the CIS.


