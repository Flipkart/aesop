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
package com.flipkart.aesop.adapter;

import com.flipkart.aesop.event.implementation.DestinationEvent;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.InitializingBean;

/**
 * This class is a utility class provided to adapt the destination Event to an Entity which you want to process.
 * Note: To use this, it assumes that the FieldMap Pair has been correctly transformed ( processable by
 * object Mapper) either in the
 * pre-mapping phase or the post mapping phase of the AbstractEvent.
 * @param <T>
 */
public class EntityAdapter<T> implements InitializingBean
{
    /* Class Type */
    private Class<T> entityClazz;

    /* Class name of the Entity */
    private String clazzName;

    /* Object Mapper to convert Field map to the entity */
    private ObjectMapper objectMapper;

    /**
     *  This Method Adapts the Destination Event to the Entity Of Your choice
     * @param destinationEvent
     * @return
     * @throws Exception
     */
    public T adaptDestinationEvent(DestinationEvent destinationEvent) throws Exception
    {
        return objectMapper.convertValue(destinationEvent.getFieldMapPair(),entityClazz);
    }

    @SuppressWarnings({"unchecked"})
    public void afterPropertiesSet() throws Exception
    {
        entityClazz = (Class<T>) Class.forName(clazzName);

        if (objectMapper == null) { objectMapper = new ObjectMapper();}
    }

    public void setClazzName(String clazzName)
    {
        this.clazzName = clazzName;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
}
