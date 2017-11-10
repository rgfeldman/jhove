# damsTools
DAMS Integration Tools

The DAMS integration tools consists of two processes in this shared repository:
VFCU (Volume File Copy Utility) and CDIS (Collection DAMS Integration System).

When used in Conjuction with CDIS, VFCU provides the automation of ingesting large number of files into DAMS.
CDIS provides for automated integration of mediaRecords between the CIS and DAMS.

Both processes are run on a scheduler (such as cron) throughout the day, but can also be manually executed.
The batch process operation types are as follows:

VFCU processes:

* watcher	   (The first step of the VFCU process. Traverses the directory tree at the defined pickup location for new .md5 files. The contents of each md5 file is recorded in the database)

* copyValidate (The second step of the VFCU process. Transfers the files from the defined pickup location, and validatates the file structure for completeness and well-formedness)

* report       (generates an email report listing the errors and successes for a particular batch of files).


CDIS processes:

* cisUpdate          (updates select values in the CIS based on values in DAMS).

* createCisMedia   (Adds a brand new CIS media record based on an existing DAMS image and attaches it to an existing object in the CIS).

* linkCisRecord        (associates the CIS record to a DAMS record in the CDIS mapping tables).  

* linkDamsRecord	   (records the DAMS record to a CDIS record in the CDIS mapping tables). 

* metadataSync     (Brings over metadata from the CIS into the DAMS).

* thumbnailSync    (replaces the thumbnail image in the CIS with the current thumbnail image in the DAMS system).

* timeFrameReport  (generates and emails report showing the status/completion/errors of files through CDIS for a given timeframe)

* sendToHotFolder  (Copies media to the appropriate DAMS hotfolder so it can be ingested into DAMS).

* vfcuDirReport   (generates and emails report showing the status/completion/errors of files through CDIS for a given vfcu source directory)

The java code for ALL of these execution types is found in this repository and is to be compiled into a single .jar file.
Each SI unit uses different combinations of the above operation types to fully integrate the DAMS with the CIS.
