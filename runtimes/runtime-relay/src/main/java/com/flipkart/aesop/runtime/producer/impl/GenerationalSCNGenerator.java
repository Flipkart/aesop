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
package com.flipkart.aesop.runtime.producer.impl;

import com.flipkart.aesop.runtime.producer.spi.SCNGenerator;
import com.linkedin.databus.core.Checkpoint;

/**
 * <code>GenerationalSCNGenerator</code> is an implementation of the {@link SCNGenerator} that handles mastership change 
 * as monotonically increasing generation changes to create relay SCNs from local SCNs and host identifiers. 
 * This SCN generator works on the following assumptions/approach:
 * <pre>
 * 	<li>The local SCN has the format : high 32 bits derived from file location reference of local SCN, low 32 bits is the offset
 *      inside the file
 *  </li>
 *  <li>
 *      The generated relay SCN has the format : high 16 bits derived from generation, next 16 bits derived from location reference of
 *      local SCN, low 32 bits is the offset inside the file
 *  </li>
 * <pre>
 * @author Regunath B
 * @version 1.0, 25 Mar 2015
 */
public class GenerationalSCNGenerator implements SCNGenerator {
	
	/** Invariants for no current generation and host*/
	private static final int NO_GENERATION = -1;
	private static final String NO_HOST = "";
	
	/** The current generation*/
	private int currentGeneration = NO_GENERATION;	
	/** The current host identifier*/
	private String currentHostId = NO_HOST;	

	/**
	 * 
	 * @see com.flipkart.aesop.runtime.producer.SCNGenerator#getSCN(long, java.lang.String)
	 */
	public long getSCN(long localSCN, String hostId) {
		if (!hostId.equalsIgnoreCase(this.currentHostId) || this.currentGeneration == NO_GENERATION) {
			this.currentGeneration += 1;
			Checkpoint cp = new Checkpoint();
		}
		long fileId = localSCN >> 32;
		long mask = (long)Integer.MAX_VALUE;
		long offset = localSCN & mask;			
		long scn = this.currentGeneration;
		scn <<= 8;
		scn |= fileId;
		scn <<= 24;
		scn |= offset;
		return scn;
	}
	
}
