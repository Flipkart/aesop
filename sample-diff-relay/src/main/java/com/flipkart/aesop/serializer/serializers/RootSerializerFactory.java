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
package com.flipkart.aesop.serializer.serializers;


import com.flipkart.aesop.serializer.model.UserInfo;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;


/**
 * The <code>RootSerializerFactory</code> class is an implementation of the Zeno {@link SerializerFactory} that creates and returns the {@link NFTypeSerializer} for
 * the root example model type {@link UserInfo}.
 * 
 * @author Regunath B
 * @version 1.0, 28 Feb 2014
 */
public class RootSerializerFactory implements SerializerFactory {

	/**
	 * Interface method implementation.
	 * @see com.netflix.zeno.serializer.SerializerFactory#createSerializers()
	 */
	public NFTypeSerializer<?>[] createSerializers() {
		NFTypeSerializer<UserInfo> serializer = new UserInfoSerializer();
        return new NFTypeSerializer<?>[] {serializer};	
    }
}
