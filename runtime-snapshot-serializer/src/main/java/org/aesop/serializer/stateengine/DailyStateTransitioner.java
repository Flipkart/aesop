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
package org.aesop.serializer.stateengine;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.aesop.serializer.SerializerConstants;
import org.trpr.platform.core.PlatformException;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;

/**
 * The <code>DailyStateTransitioner</code> class is an implementation of {@link StateTransitioner} that creates or suitably initializes an existing 
 * {@link FastBlobStateEngine} for producing daily snapshots followed by any number of deltas for the day.
 * 
 * @see StateTransitioner
 * @author Regunath B
 * @version 1.0, 5 March 2014
 */

public class DailyStateTransitioner extends StateTransitioner {
	
	private SimpleDateFormat DAILY_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

	/** The FastBlobStateEngine that this StateTransitioner creates and manages */
	private FastBlobStateEngine stateEngine;
	
	/**
	 * Abstract method implementation. Creates a new FastBlobStateEngine, if required, and 
	 * loads the last daily snapshot, if one is available
	 * @see org.aesop.serializer.stateengine.StateTransitioner#getStateEngine()
	 */
	public FastBlobStateEngine getStateEngine() {
		if (this.stateEngine == null) {
			this.stateEngine = new FastBlobStateEngine(this.serializerFactory);
		}		
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DATE, -1);
		String dayMinusOne = DAILY_FORMAT.format(calendar.getTime());
		File previousSnapshot = new File (snapshotsLocationFile, SerializerConstants.SNAPSHOT_LOCATION + dayMinusOne);
		if (previousSnapshot.exists()) {
			FastBlobReader previousSnapshotReader = new FastBlobReader(this.stateEngine);
			try {
				previousSnapshotReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(previousSnapshot))));
			} catch (Exception e) {
				throw new PlatformException("Error reading snapshot from file : " + previousSnapshot.getAbsolutePath(), e);
			}
		}
		return this.stateEngine;
	}

}
