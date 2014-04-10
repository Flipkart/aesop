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
	private void readSnapshotAndDeltas(FastBlobStateEngine stateEngine, final long sinceSCN, final boolean limitToSCN) {
		File[] stateDirs = this.serializedDataLocationDir.listFiles(new FilenameFilter() {
		    public boolean accept(File dir, String name) {
		    	if (new File(dir,name).isDirectory()) {
		    		if (limitToSCN) {
		    			return Long.valueOf(name) <= getSCNDay(sinceSCN); // we are interested only in directories that are older than SCN/state engine version
		    		} else {
		    			return Long.valueOf(name) >= getSCNDay(sinceSCN); // we are interested only in directories that are newer than SCN/state engine version
		    		}
		    	}
		    	return false;
		    }
		});
		if (stateDirs.length == 0) { // no state files found and this is an initialization load
			return;
		}
		Arrays.sort(stateDirs, Collections.reverseOrder(new Comparator<File>() { // sort descending to get latest available state
			public int compare(File file1, File file2) {
				return (int)(file1.lastModified() - file2.lastModified()); 
			}			
		}));
		FastBlobReader stateReader = new FastBlobReader(stateEngine);
		for (File stateDir : stateDirs) {
			File[] stateFiles = stateDir.listFiles(new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			    	if (name.endsWith(SerializerConstants.SNAPSHOT_FILE) || name.endsWith(SerializerConstants.DELTA_FILE)) {
			    		LOGGER.debug("State file version : SCN [" + getStateFileVersion(new File(dir,name)) + " : " +sinceSCN + "]");
			    		if (limitToSCN) {
			    			return getStateFileVersion(new File(dir,name)) <= sinceSCN; // we are interested only in files that are older than SCN/state engine version
			    		} else {
			    			return getStateFileVersion(new File(dir,name)) >= sinceSCN; // we are interested only in files that are newer than SCN/state engine version
			    		}
			    	}
			    	return false;
			    }
			});
			for (File stateFile : stateFiles) {
				try {
					LOGGER.debug("State file version : engine version [" + this.getStateFileVersion(stateFile) + " : " + this.getStateEngineVersion(stateEngine) + 
							"] State file is " + stateFile.getAbsolutePath());
					if (this.getStateFileVersion(stateFile) > this.getStateEngineVersion(stateEngine)) { // we want to load the state data only if the engine is not at this version already
						if (stateFile.getName().endsWith(SerializerConstants.SNAPSHOT_FILE)) {
							stateReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(stateFile))));
						} else {
							stateReader.readDelta(new DataInputStream(new BufferedInputStream(new FileInputStream(stateFile))));
						}
						stateEngine.setLatestVersion(String.valueOf(this.getStateFileVersion(stateFile)));
						LOGGER.info("State engine initialized from state file : " + stateFile.getAbsolutePath());
					}
				} catch (Exception e) { // The state data read has failed. Proceed with empty state or next available set of state files
					LOGGER.warn("Error reading state file : " + stateFile.getAbsolutePath() + " Proceeding with next file. Error message is : " + e.getMessage(), e);
				}
			}
		}
		LOGGER.debug(this.getStateEngineVersion(stateEngine) != 0 ? "State engine is set to version : " + stateEngine.getLatestVersion() : 
				"State engine not initialized from any existing snapshot");
	}
	
	/** Helper method to get state file version of the form yyyyMMdd*/
	private long getStateFileVersion(File stateFile) {
		return Long.valueOf(stateFile.getName().substring(0, SerializerConstants.DAILY_FILE_FORMAT_STRING.length()));
	}	
	/** Helper method to get only the day portion of the SCN of the form yyyyMMdd*/
	private long getSCNDay(long sinceSCN) {
		return sinceSCN == 0L ? sinceSCN : 
			Long.valueOf(String.valueOf(sinceSCN).substring(0, SerializerConstants.DAILY_DIR_FORMAT_STRING.length()));
	}	
	/** Helper method to get the engine version*/
	private long getStateEngineVersion(FastBlobStateEngine stateEngine) {
		return Long.valueOf(((stateEngine.getLatestVersion() == null || stateEngine.getLatestVersion().trim().length() == 0) ? 
						"0" : stateEngine.getLatestVersion()));
	}
}
