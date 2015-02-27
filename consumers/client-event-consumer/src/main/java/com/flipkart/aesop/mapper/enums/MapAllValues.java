/*******************************************************************************
 *
 * Copyright 2012-2015, the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obta a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 *******************************************************************************/

package com.flipkart.aesop.mapper.enums;

import com.flipkart.aesop.mapper.utils.MapperConstants;

/**
 * Enum to denote if {@link MapperConstants#MAP_ALL_FIELD_NAME} is set to true or not in the HOCON-config for the
 * namespace, entity or columns.
 * @author Prakhar Jain
 */
public enum MapAllValues
{
	/** mapAll is set to true. */
	TRUE,
	/** mapAll is set ot false. */
	FALSE
}
