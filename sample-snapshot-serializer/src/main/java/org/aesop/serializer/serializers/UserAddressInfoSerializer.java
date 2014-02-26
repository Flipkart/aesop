package org.aesop.serializer.serializers;

import java.util.Collection;

import org.aesop.serializer.model.UserAddressInfo;

import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

public class UserAddressInfoSerializer extends NFTypeSerializer<UserAddressInfo> {

	public UserAddressInfoSerializer() {
		super(UserAddressInfo.class.getName());
	}

	@Override
	protected FastBlobSchema createSchema() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected UserAddressInfo doDeserialize(NFDeserializationRecord arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void doSerialize(UserAddressInfo arg0, NFSerializationRecord arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
		// TODO Auto-generated method stub
		return null;
	}

}
