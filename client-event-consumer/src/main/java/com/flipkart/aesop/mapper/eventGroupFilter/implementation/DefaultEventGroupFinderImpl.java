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

package com.flipkart.aesop.mapper.eventGroupFilter.implementation;

import com.flipkart.aesop.mapper.eventGroupFilter.AbstractEventGroupFinder;
import com.flipkart.aesop.mapper.eventGroupFilter.EventGroupFinder;

/**
 * Default Implementation of {@link EventGroupFinder}.
 * @author Prakhar Jain
 */
public class DefaultEventGroupFinderImpl extends AbstractEventGroupFinder
{
	/** Helper class to create Single instance of this class thread-safely. */
	private static class SingletonHelper
	{
		/** {@link DefaultEventGroupFinderImpl} instance creation. */
		private static final DefaultEventGroupFinderImpl INSTANCE = new DefaultEventGroupFinderImpl();
	}

	/**
	 * Loads {@link SingletonHelper} and Gets Instance of this class.
	 * @return Instance
	 */
	public static DefaultEventGroupFinderImpl getInstance()
	{
		return SingletonHelper.INSTANCE;
	}

	@Override
	public Integer getEventGroupNo(String namespaceEntity, Integer totalDestinationGroups)
	{
		if (namespaceEntity == null)
		{
			return defaultGroup;
		}
		Integer hashCode = namespaceEntity.hashCode();

		return (hashCode % totalDestinationGroups) + 1;
	}
}
