/*
 * Copyright 2012-2015, the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.aesop.serializer.stateengine;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;

import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.SerializerConstants;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;

/**
 * The <code>DailyStateTransitioner</code> class is a sub-type of {@link StateTransitioner} that creates or suitably initializes an existing 
 * {@link FastBlobStateEngine} for producing daily snapshots followed by any number of deltas for the day.
 * 
 * @see StateTransitioner
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */

public class DailyStateTransitioner extends StateTransitioner {
	
	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(DailyStateTransitioner.class);
	
	/** The FastBlobStateEngine that this StateTransitioner creates and manages */
	private FastBlobStateEngine stateEngine;
	
	/**
	 * Abstract method implementation. Creates a new FastBlobStateEngine, if required, and 
	 * loads the last daily snapshot, if one is available
	 * @see com.flipkart.aesop.serializer.stateengine.StateTransitioner#getStateEngine()
	 */
	public FastBlobStateEngine getStateEngine() {
		return this.stateEngine;
	}
	
	/**
	 * Abstract method implementation. Persists state held by the FastBlobStateEngine and prepares it for the next cycle
	 * @see com.flipkart.aesop.serializer.stateengine.StateTransitioner#saveState()
	 */
	public void saveState() {
		
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		
		this.getStateEngine().prepareForWrite();
	    // Create a writer, which will be responsible for creating snapshot and/or delta blobs.
	    FastBlobWriter writer = new FastBlobWriter(this.getStateEngine());				
		DataOutputStream dataOS = null;
		
		try {
			Calendar calendar = Calendar.getInstance();
			String today = SerializerConstants.DAILY_FORMAT.format(calendar.getTime());		
			File snapshotFile = new File (snapshotsLocationFile, SerializerConstants.SNAPSHOT_LOCATION + SerializerConstants.DELIM_CHAR + today);
			if (!snapshotFile.exists()) { // no snapshot for today, write a snapshot for today and return
				snapshotFile.createNewFile();
				dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotFile)));
				writer.writeSnapshot(dataOS);			
				LOGGER.info("Fast blob state engine data written to snapshot file : " + snapshotFile.getAbsolutePath());	    
			} else { // write delta
				File deltaLocationDir = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);	
				File deltaFile = new File(deltaLocationDir, (SerializerConstants.DELTA_LOCATION + SerializerConstants.DELIM_CHAR +  today 
						+ SerializerConstants.DELIM_CHAR +  SerializerConstants.HOURLY_MINUTE_FORMAT.format(calendar.getTime())));				
				deltaFile.createNewFile();
				dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(deltaFile)));
				writer.writeDelta(dataOS);
				LOGGER.info("Fast blob state engine data written to delta file : " + deltaFile.getAbsolutePath());	    
			}
		    dataOS.close();
			this.getStateEngine().prepareForNextCycle(); // prepare the state engine for next run
		} catch (Exception e) {
			throw new PlatformException("Error saving state engine data : " + e.getMessage(), e);
		}	    
	}
	
	/**
	 * Overriden super class method. Additionally creates and initializes the FastBlobStateEngine
	 * @see com.flipkart.aesop.serializer.stateengine.StateTransitioner#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {		
		super.afterPropertiesSet();		
		this.stateEngine = new FastBlobStateEngine(this.serializerFactory);
		this.readLatestSnapshotAndDeltas();
	}
	
	/**
	 * Reads the latest daily snapshot and its deltas
	 */
	private void readLatestSnapshotAndDeltas() {
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationDir = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationDir = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);			
		File[] snapshotFiles = snapshotsLocationDir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		        return name.toLowerCase().startsWith(SerializerConstants.SNAPSHOT_LOCATION);
		    }
		});
		if (snapshotFiles.length == 0) {
			return;
		}
		Arrays.sort(snapshotFiles, Collections.reverseOrder(new Comparator<File>() { // sort descending
			public int compare(File file1, File file2) {
				return (int)(file1.lastModified() - file2.lastModified()); 
			}			
		}));
		FastBlobReader previousStateReader = new FastBlobReader(this.stateEngine);
		for (File snapshotFile : snapshotFiles) {
			try {
				previousStateReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile))));
				this.readDeltasForSnapshot(snapshotFile, deltaLocationDir, previousStateReader);
				LOGGER.info("State engine initialized from deltas and snapshot of snapshot file : " + snapshotFile.getAbsolutePath());
				break;
			} catch (Exception e) { // The snapshot read has failed. Proceed with empty state or next available snapshot
				LOGGER.warn("Error reading snapshot and deltas for file : {}. Error message is : {}",snapshotFile.getAbsolutePath(), e.getMessage());
			}
		}
		LOGGER.info(this.stateEngine.getLatestVersion() != null ? "State engine initialized to version : " + this.stateEngine.getLatestVersion() : 
			"State engine not initialized from any existing snapshot");
	}
	
	/**
	 * Reads the deltas for the specified snapshot file from the specified delta location directory into the specified fast blob reader
	 * @param snapshotFile the snapshot file to read deltas for
	 * @param deltaLocationDir the delta files location dir
	 * @param previousStateReader the FastBlobReader used to read the deltas
	 */
	private void readDeltasForSnapshot(final File snapshotFile, File deltaLocationDir, FastBlobReader previousStateReader) {
		File[] deltaFiles = deltaLocationDir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		    	String snapshotDay = snapshotFile.getName().substring((SerializerConstants.SNAPSHOT_LOCATION + SerializerConstants.DELIM_CHAR).length());
		        return name.toLowerCase().startsWith((SerializerConstants.DELTA_LOCATION + SerializerConstants.DELIM_CHAR +  snapshotDay));
		    }
		});
		if (deltaFiles.length == 0) {
			this.stateEngine.setLatestVersion(this.getSnapshotVersion(snapshotFile)); //set the state engine version to snapshot suffix			
			return;
		}
		Arrays.sort(deltaFiles,new Comparator<File>() { // sort ascending
			public int compare(File file1, File file2) {
				return (int)(file1.lastModified() - file2.lastModified()); 
			}			
		});		
		for (File deltaFile : deltaFiles) {
			try {
				previousStateReader.readDelta(new DataInputStream(new BufferedInputStream(new FileInputStream(deltaFile))));
				LOGGER.info("State engine delta loaded from file : " + deltaFile.getAbsolutePath());
			} catch (Exception e) { // Unable to read all of the delta files. Abort it
				LOGGER.warn("Error reading delta from file : {}. Error is {}",deltaFile.getAbsolutePath(), e.getMessage());
				throw new PlatformException("Error reading delta from file :" + deltaFile.getAbsolutePath() + " Error is : " +  e.getMessage());
			}
		}
		this.stateEngine.setLatestVersion(this.getDeltaVersion(deltaFiles[deltaFiles.length - 1])); // set the state engine version to the last read delta file	suffix	
	}
	
	/** Helper method to get state engine version from snapshot file. Normalized to the form yyyyMMddHHmm*/
	private String getSnapshotVersion(File snapshotFile) {
		return snapshotFile.getName().substring(
				(SerializerConstants.SNAPSHOT_LOCATION + SerializerConstants.DELIM_CHAR).length()).replace(
						SerializerConstants.DELIM_CHAR, SerializerConstants.EMPTY_CHAR) + SerializerConstants.ZERO_HH_MM;
	}	
	/** Helper method to get state engine version from delta file. Normalized to the form yyyyMMddHHmm*/
	private String getDeltaVersion(File deltaFile) {
		return deltaFile.getName().substring(
				(SerializerConstants.DELTA_LOCATION + SerializerConstants.DELIM_CHAR).length()).replace(
						SerializerConstants.DELIM_CHAR, SerializerConstants.EMPTY_CHAR);
	}
	
}
