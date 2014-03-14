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
 * User Address Info domain object
 * @author Regunath B
 *
 */
public class UserAddressInfo {
    private String id;
    private String account_id;
    private String first_name;
    private String last_name;
    private String address_line1;
    private String address_line2;
    private String landmark;
    private String city;
    private String state;
    private String state_code;
    private String country;
    private String pincode;
    private String phone;
    private boolean guest;
    private boolean active;
    private int version;
    private String creation_date;
    private String last_modified;
    private String creating_system;
    private Map<String,Object> preferences;
    
    public UserAddressInfo() {    	
    }
    
    /** Constructor from all fields*/    
    public UserAddressInfo(String id, String account_id, String first_name,
			String last_name, String address_line1, String address_line2,
			String landmark, String city, String state, String state_code,
			String country, String pincode, String phone, boolean guest,
			boolean active, int version, String creation_date,
			String last_modified, String creating_system,
			Map<String, Object> preferences) {
		super();
		this.id = id;
		this.account_id = account_id;
		this.first_name = first_name;
		this.last_name = last_name;
		this.address_line1 = address_line1;
		this.address_line2 = address_line2;
		this.landmark = landmark;
		this.city = city;
		this.state = state;
		this.state_code = state_code;
		this.country = country;
		this.pincode = pincode;
		this.phone = phone;
		this.guest = guest;
		this.active = active;
		this.version = version;
		this.creation_date = creation_date;
		this.last_modified = last_modified;
		this.creating_system = creating_system;
		this.preferences = preferences;
	}
    
	/**Getter/Setter methods */    
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getAccount_id() {
		return account_id;
	}
	public void setAccount_id(String account_id) {
		this.account_id = account_id;
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
	public String getAddress_line1() {
		return address_line1;
	}
	public void setAddress_line1(String address_line1) {
		this.address_line1 = address_line1;
	}
	public String getAddress_line2() {
		return address_line2;
	}
	public void setAddress_line2(String address_line2) {
		this.address_line2 = address_line2;
	}
	public String getLandmark() {
		return landmark;
	}
	public void setLandmark(String landmark) {
		this.landmark = landmark;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getState_code() {
		return state_code;
	}
	public void setState_code(String state_code) {
		this.state_code = state_code;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getPincode() {
		return pincode;
	}
	public void setPincode(String pincode) {
		this.pincode = pincode;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public boolean isGuest() {
		return guest;
	}
	public void setGuest(boolean guest) {
		this.guest = guest;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
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
	public Map<String, Object> getPreferences() {
		return preferences;
	}
	public void setPreferences(Map<String, Object> preferences) {
		this.preferences = preferences;
	}
        
}
