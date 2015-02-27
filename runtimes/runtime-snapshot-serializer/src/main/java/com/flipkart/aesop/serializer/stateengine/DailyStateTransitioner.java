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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Calendar;

import org.trpr.platform.core.PlatformException;
import org.trpr.platform.core.impl.logging.LogFactory;
import org.trpr.platform.core.spi.logging.Logger;

import com.flipkart.aesop.serializer.SerializerConstants;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobWriter;

/**
 * The <code>DailyStateTransitioner</code> class is a sub-type of {@link StateTransitioner} that creates or suitably initializes an existing 
 * {@link FastBlobStateEngine} for producing daily snapshots followed by any number of deltas for the day.
 * 
 * @see StateTransitioner
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */

public class DailyStateTransitioner<T> extends StateTransitioner<T> {
	
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
		this.getStateEngine().prepareForWrite();
	    // Create a writer, which will be responsible for creating snapshot and/or delta blobs.
	    FastBlobWriter writer = new FastBlobWriter(this.getStateEngine());				
		DataOutputStream dataOS = null;		
		try {
			Calendar calendar = Calendar.getInstance();
			File dayDir = new File (this.serializedDataLocationDir, SerializerConstants.DAILY_DIR_FORMAT.format(calendar.getTime()));
			if (!dayDir.exists() || this.stateEngine.getLatestVersion() == null || this.stateEngine.getLatestVersion().trim().length() == 0) { 
				// no snapshot for today or state engine contents has not been written out before, write a snapshot for today and return
				dayDir.mkdir();
				File snapshotFile = new File (dayDir, SerializerConstants.DAILY_FILE_FORMAT.format(calendar.getTime()) + SerializerConstants.DELIM_CHAR
						+ SerializerConstants.SNAPSHOT_FILE);
				snapshotFile.createNewFile();
				dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(snapshotFile)));
				writer.writeSnapshot(dataOS);			
				LOGGER.info("Fast blob state engine data written to snapshot file : " + snapshotFile.getAbsolutePath());	    
			} else { // write delta
				File deltaFile = new File (dayDir, SerializerConstants.DAILY_FILE_FORMAT.format(calendar.getTime()) + SerializerConstants.DELIM_CHAR
						+ SerializerConstants.DELTA_FILE);				
				deltaFile.createNewFile();
				dataOS = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(deltaFile)));
				writer.writeDelta(dataOS);
				LOGGER.info("Fast blob state engine data written to delta file : " + deltaFile.getAbsolutePath());	    
			}
		    dataOS.close();
			// set the state engine version to the snapshot or delta file that was written
			this.stateEngine.setLatestVersion(SerializerConstants.DAILY_FILE_FORMAT.format(calendar.getTime()));
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
	}
	
}
