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
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.SerializerConstants;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;

/**
 * The <code>DailyStateTransitioner</code> class is an implementation of {@link StateTransitioner} that creates or suitably initializes an existing 
 * {@link FastBlobStateEngine} for producing daily snapshots followed by any number of deltas for the day.
 * 
 * @see StateTransitioner
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */

public class DailyStateTransitioner extends StateTransitioner {
	
	/** The Logger interface*/
	private static final Logger LOGGER = LogFactory.getLogger(DailyStateTransitioner.class);
	
	/** The Date format instances for used in naming files and state engine versions*/
	public SimpleDateFormat DAILY_FORMAT = new SimpleDateFormat("_yyyy-MM-dd");
	public SimpleDateFormat HOURLY_MINUTE_FORMAT = new SimpleDateFormat("_HH-mm");

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
			String today = DAILY_FORMAT.format(calendar.getTime());		
			File snapshotFile = new File (snapshotsLocationFile, SerializerConstants.SNAPSHOT_LOCATION + today);
			if (!snapshotFile.exists()) { // no snapshot for today, write a snapshot for today and return
				snapshotFile.createNewFile();
				dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotFile)));
				writer.writeSnapshot(dataOS);			
				LOGGER.info("Fast blob state engine data written to snapshot file : " + snapshotFile.getAbsolutePath());	    
			} else {
				File deltaLocationFile = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);	
				File deltaFile = new File(deltaLocationFile, (SerializerConstants.DELTA_LOCATION + today + HOURLY_MINUTE_FORMAT.format(calendar.getTime())));				
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
	 * Overriden super class method. Additionall creates and initializes the FastBlobStateEngine
	 * @see com.flipkart.aesop.serializer.stateengine.StateTransitioner#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		
		super.afterPropertiesSet();
		
		this.stateEngine = new FastBlobStateEngine(this.serializerFactory);
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		
		Calendar calendar = Calendar.getInstance();
		String today = DAILY_FORMAT.format(calendar.getTime());		
		// check if a snapshot exists for today and load it
		File previousSnapshot = new File (snapshotsLocationFile, SerializerConstants.SNAPSHOT_LOCATION + today);
		if (previousSnapshot.exists()) {
			FastBlobReader previousSnapshotReader = new FastBlobReader(this.stateEngine);
			try {
				previousSnapshotReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(previousSnapshot))));
			} catch (Exception e) {
				throw new PlatformException("Error reading current day snapshot from file : " + previousSnapshot.getAbsolutePath(), e);
			}
			this.stateEngine.setLatestVersion(today);
		} else { // check if a previous day snapshot exists		
			calendar.add(Calendar.DATE, -1);
			String dayMinusOne = DAILY_FORMAT.format(calendar.getTime());
			previousSnapshot = new File (snapshotsLocationFile, SerializerConstants.SNAPSHOT_LOCATION + dayMinusOne);
			if (previousSnapshot.exists()) {
				FastBlobReader previousSnapshotReader = new FastBlobReader(this.stateEngine);
				try {
					previousSnapshotReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(previousSnapshot))));
				} catch (Exception e) {
					throw new PlatformException("Error reading previous day snapshot from file : " + previousSnapshot.getAbsolutePath(), e);
				}
			}
			this.stateEngine.setLatestVersion(dayMinusOne);
		}		
	}

}
