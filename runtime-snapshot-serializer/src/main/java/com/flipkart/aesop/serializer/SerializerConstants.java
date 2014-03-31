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
package com.flipkart.aesop.serializer;

import java.text.SimpleDateFormat;

/**
 * The <code>SerializerConstants</code> class is a place-holder for serializer
 * module constants.
 * 
 * @author Regunath B
 * @version 1.0, 24 Feb 2014
 */
public class SerializerConstants {

	/** Constant string literals for time related values*/
	public static final String DAILY_DIR_FORMAT_STRING = "yyyyMMdd";
	public static final String DAILY_FILE_FORMAT_STRING = "yyyyMMddHHmmss";
	
	/** File name suffices for snapshot and deltas */
	public static final String SNAPSHOT_FILE = "snapshot";
	public static final String DELTA_FILE = "delta";	

	/** Delimiter chars used in file name handling*/
	public static final String DELIM_CHAR = "_";

	/** The Date format instances for used in naming files and state engine versions*/
	public static SimpleDateFormat DAILY_DIR_FORMAT = new SimpleDateFormat(DAILY_DIR_FORMAT_STRING);
	public static SimpleDateFormat DAILY_FILE_FORMAT = new SimpleDateFormat(DAILY_FILE_FORMAT_STRING);
		
}
