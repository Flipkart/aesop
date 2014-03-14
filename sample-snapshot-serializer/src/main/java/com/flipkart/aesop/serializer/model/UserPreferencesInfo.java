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
package com.flipkart.aesop.serializer.model;

import java.util.Map;

/**
 * User Preferences Info domain object
 * @author Regunath B
 *
 */
public class UserPreferencesInfo {
    private String id;
    private String preferences_name;
    private Map<String,Object> value;
    private int version;
    private String last_modified;
    
    public UserPreferencesInfo() {    	
    }
    
    /** Constructor from all fields*/
    public UserPreferencesInfo(String id, String preferences_name,
			Map<String, Object> value, int version, String last_modified) {
		super();
		this.id = id;
		this.preferences_name = preferences_name;
		this.value = value;
		this.version = version;
		this.last_modified = last_modified;
	}
    
	/**Getter/Setter methods */        
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPreferences_name() {
		return preferences_name;
	}
	public void setPreferences_name(String preferences_name) {
		this.preferences_name = preferences_name;
	}
	public Map<String, Object> getValue() {
		return value;
	}
	public void setValue(Map<String, Object> value) {
		this.value = value;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getLast_modified() {
		return last_modified;
	}
	public void setLast_modified(String last_modified) {
		this.last_modified = last_modified;
	}    
}
