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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * User Info domain object
 * @author Regunath B
 *
 */
public class UserInfo {
    private String id;
    private String primary_account_id;
    private String first_name;
    private String last_name;
    private String primary_email;
    private String primary_phone;
    private String profile_name;
    private String blacklisted_parent;

    private String status;
    private boolean active;
    private boolean guest;
    private Boolean blacklisted = null;

    private Map<String,UserPreferencesInfo> preferences;
    private Set<UserAddressInfo> addresses;
    private List<String> merged_account_ids;
    private int version;
    private String creation_date;
    private String last_modified;
    private String creating_system;
    
    public UserInfo() {    	 
    }
    
    /** Constructor from all fields*/
    public UserInfo(String id, String primary_account_id, String first_name,
			String last_name, String primary_email, String primary_phone,
			String profile_name, String blacklisted_parent, String status,
			boolean active, boolean guest, Boolean blacklisted,
			Map<String, UserPreferencesInfo> preferences,
			Set<UserAddressInfo> addresses, List<String> merged_account_ids,
			int version, String creation_date, String last_modified,
			String creating_system) {
		super();
		this.id = id;
		this.primary_account_id = primary_account_id;
		this.first_name = first_name;
		this.last_name = last_name;
		this.primary_email = primary_email;
		this.primary_phone = primary_phone;
		this.profile_name = profile_name;
		this.blacklisted_parent = blacklisted_parent;
		this.status = status;
		this.active = active;
		this.guest = guest;
		this.blacklisted = blacklisted;
		this.preferences = preferences;
		this.addresses = addresses;
		this.merged_account_ids = merged_account_ids;
		this.version = version;
		this.creation_date = creation_date;
		this.last_modified = last_modified;
		this.creating_system = creating_system;
	}
    
	/** Getter/Setter methods*/    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getPrimary_account_id() {
		return primary_account_id;
	}
	public void setPrimary_account_id(String primary_account_id) {
		this.primary_account_id = primary_account_id;
	}
	public String getFirst_name() {
		return first_name;
	}
	public void setFirst_name(String first_name) {
		this.first_name = first_name;
	}
	public String getLast_name() {
		return last_name;
	}
	public void setLast_name(String last_name) {
		this.last_name = last_name;
	}
	public String getPrimary_email() {
		return primary_email;
	}
	public void setPrimary_email(String primary_email) {
		this.primary_email = primary_email;
	}
	public String getPrimary_phone() {
		return primary_phone;
	}
	public void setPrimary_phone(String primary_phone) {
		this.primary_phone = primary_phone;
	}
	public String getProfile_name() {
		return profile_name;
	}
	public void setProfile_name(String profile_name) {
		this.profile_name = profile_name;
	}
	public String getBlacklisted_parent() {
		return blacklisted_parent;
	}
	public void setBlacklisted_parent(String blacklisted_parent) {
		this.blacklisted_parent = blacklisted_parent;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public boolean isGuest() {
		return guest;
	}
	public void setGuest(boolean guest) {
		this.guest = guest;
	}
	public Boolean getBlacklisted() {
		return blacklisted;
	}
	public void setBlacklisted(Boolean blacklisted) {
		this.blacklisted = blacklisted;
	}
	public Map<String, UserPreferencesInfo> getPreferences() {
		return preferences;
	}
	public void setPreferences(Map<String, UserPreferencesInfo> preferences) {
		this.preferences = preferences;
	}
	public Set<UserAddressInfo> getAddresses() {
		return addresses;
	}
	public void setAddresses(Set<UserAddressInfo> addresses) {
		this.addresses = addresses;
	}
	public List<String> getMerged_account_ids() {
		return merged_account_ids;
	}
	public void setMerged_account_ids(List<String> merged_account_ids) {
		this.merged_account_ids = merged_account_ids;
	}
	public int getVersion() {
		return version;
	}
	public void setVersion(int version) {
		this.version = version;
	}
	public String getCreation_date() {
		return creation_date;
	}
	public void setCreation_date(String creation_date) {
		this.creation_date = creation_date;
	}
	public String getLast_modified() {
		return last_modified;
	}
	public void setLast_modified(String last_modified) {
		this.last_modified = last_modified;
	}
	public String getCreating_system() {
		return creating_system;
	}
	public void setCreating_system(String creating_system) {
		this.creating_system = creating_system;
	}
        
}
