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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import org.apache.avro.generic.GenericRecord;
import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.SerializerConstants;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;

/**
 * The <code>DailyDiffInterpreter</code> is a sub-type of the {@link DiffInterpreter} that interprets change events from daily snapshots followed by 
 * any number of deltas for the day.
 * 
 * @author Regunath B
 * @version 1.0, 19 March 2014
 */

public class DailyDiffInterpreter<T, S extends GenericRecord> extends DiffInterpreter<T,S> {

	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(DailyDiffInterpreter.class);
	
	/**
	 * Overriden superclass method. Reads daily snapshot and associated deltas to update the state engine to a state identified by the SCN specified
	 * @see com.flipkart.aesop.serializer.stateengine.DiffInterpreter#readSnapshotAndDeltasForSCN(com.netflix.zeno.fastblob.FastBlobStateEngine, long)
	 */
	protected void readSnapshotAndDeltasForSCN(FastBlobStateEngine stateEngine,long sinceSCN) {
		this.readSnapshotAndDeltas(stateEngine, sinceSCN, true);
	}

	protected void readSnapshotAndDeltasAfterSCN(FastBlobStateEngine stateEngine, long sinceSCN) {
		this.readSnapshotAndDeltas(stateEngine, sinceSCN, false);
	}

	/**
	 * Reads snapshots and deltas for the specified SCN.
	 * @param stateEngine the FastBlobStateEngine to append snapshots and deltas to
	 * @param sinceSCN the SCN for identifying snapshots and deltas
	 * @param limitToSCN boolean true if the read should stop at the snapshot and related deltas or proceed to all available later state changes
	 */
	private void readSnapshotAndDeltas(FastBlobStateEngine stateEngine, long sinceSCN, final boolean limitToSCN) {
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationDir = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationDir = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);		
		final long oldestState = Math.min(this.getStateEngineVersionDay(stateEngine), sinceSCN); // be conservative and take earliest state in order to not miss any updates
		File[] snapshotFiles = snapshotsLocationDir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		    	if (name.toLowerCase().startsWith(SerializerConstants.SNAPSHOT_LOCATION)) {
		    		if (limitToSCN) {
		    			return getSnapshotVersion(new File(name)) <= oldestState; // we are interested only in files that are older than SCN/state engine version
		    		} else {
		    			return getSnapshotVersion(new File(name)) >= oldestState; // we are interested only in files that are newer than SCN/state engine version
		    		}
		    	}
		        return false;
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
		FastBlobReader previousStateReader = new FastBlobReader(stateEngine);
		for (File snapshotFile : snapshotFiles) {
			try {
				if (this.getSnapshotVersion(snapshotFile) > this.getStateEngineVersionDay(stateEngine)) { // we want to load the snapshot data only if the engine is not at this version already
					previousStateReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile))));
				}
				this.readDeltasForSnapshot(snapshotFile, deltaLocationDir, previousStateReader, stateEngine, limitToSCN);
				LOGGER.info("State engine initialized from deltas and snapshot of snapshot file : " + snapshotFile.getAbsolutePath());
				break;
			} catch (Exception e) { // The snapshot read has failed. Proceed with empty state or next available snapshot
				LOGGER.warn("Error reading snapshot and deltas for file : {}. Error message is : {}",snapshotFile.getAbsolutePath(), e.getMessage());
			}
		}
		LOGGER.info(stateEngine.getLatestVersion() != null ? "State engine initialized to version : " + stateEngine.getLatestVersion() : 
			"State engine not initialized from any existing snapshot");
	}
	
	/**
	 * Reads the deltas for the specified snapshot file from the specified delta location directory into the specified fast blob reader
	 * @param snapshotFile the snapshot file to read deltas for
	 * @param deltaLocationDir the delta files location dir
	 * @param previousStateReader the FastBlobReader used to read the deltas
	 */
	private void readDeltasForSnapshot(final File snapshotFile, File deltaLocationDir, FastBlobReader previousStateReader, FastBlobStateEngine stateEngine, final boolean limitToSCN) {
		final long engineVersion = Long.valueOf(stateEngine.getLatestVersion());
		File[] deltaFiles = deltaLocationDir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		    	String snapshotDay = snapshotFile.getName().substring((SerializerConstants.SNAPSHOT_LOCATION + SerializerConstants.DELIM_CHAR).length());		    	
		    	if (name.toLowerCase().startsWith((SerializerConstants.DELTA_LOCATION + SerializerConstants.DELIM_CHAR + snapshotDay))) {
		    		if (limitToSCN) {
		    			return getDeltaVersion(new File(name)) <= engineVersion; // we are interested only in files that are older than SCN/state engine version
		    		} else {
		    			return getDeltaVersion(new File(name)) >= engineVersion; // we are interested only in files that are newer to SCN/state engine version
		    		}
		    	}
		        return false;
		    }
		});
		if (deltaFiles.length == 0) {
			stateEngine.setLatestVersion(String.valueOf(this.getSnapshotVersion(snapshotFile))); //set the state engine version to snapshot suffix			
			return;
		}
		Arrays.sort(deltaFiles,new Comparator<File>() { // sort ascending
			public int compare(File file1, File file2) {
				return (int)(file1.lastModified() - file2.lastModified()); 
			}			
		});		
		for (File deltaFile : deltaFiles) {
			try {
				if (this.getDeltaVersion(deltaFile) > engineVersion) { // we want to load the delta data only if the engine is not at this version already				
					previousStateReader.readDelta(new DataInputStream(new BufferedInputStream(new FileInputStream(deltaFile))));
				}
				LOGGER.info("State engine delta loaded from file : " + deltaFile.getAbsolutePath());
			} catch (Exception e) { // Unable to read all of the delta files. Abort it
				LOGGER.warn("Error reading delta from file : {}. Error is {}",deltaFile.getAbsolutePath(), e.getMessage());
				throw new PlatformException("Error reading delta from file :" + deltaFile.getAbsolutePath() + " Error is : " +  e.getMessage());
			}
		}
		stateEngine.setLatestVersion(String.valueOf(this.getDeltaVersion(deltaFiles[deltaFiles.length - 1]))); // set the state engine version to the last read delta file	suffix	
	}
	
	/** Helper method to get state engine version from snapshot file. Normalized to the form yyyyMMddHHmm*/
	private long getSnapshotVersion(File snapshotFile) {
		return Long.valueOf(snapshotFile.getName().substring(
				(SerializerConstants.SNAPSHOT_LOCATION + SerializerConstants.DELIM_CHAR).length()).replace(
						SerializerConstants.DELIM_CHAR, SerializerConstants.EMPTY_CHAR) + SerializerConstants.ZERO_HH_MM);
	}	
	/** Helper method to get state engine version from delta file. Normalized to the form yyyyMMddHHmm*/
	private long getDeltaVersion(File deltaFile) {
		return Long.valueOf(deltaFile.getName().substring(
				(SerializerConstants.DELTA_LOCATION + SerializerConstants.DELIM_CHAR).length()).replace(
						SerializerConstants.DELIM_CHAR, SerializerConstants.EMPTY_CHAR));
	}
	/** Helper method to get only the day portion of the engine version. Normalized to the form yyyyMMddHHmm*/
	private long getStateEngineVersionDay(FastBlobStateEngine stateEngine) {
		return Long.valueOf(stateEngine.getLatestVersion() == null ? "0" : 
			stateEngine.getLatestVersion().substring(0, SerializerConstants.DAILY_FORMAT_STRING.length()) + SerializerConstants.ZERO_HH_MM);
	}
}
