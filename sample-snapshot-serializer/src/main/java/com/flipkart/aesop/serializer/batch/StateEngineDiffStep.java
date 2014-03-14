package com.flipkart.aesop.serializer.batch;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Arrays;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.flipkart.aesop.serializer.SerializerConstants;
import com.flipkart.aesop.serializer.model.UserInfo;
import com.flipkart.aesop.serializer.serializers.RootSerializerFactory;
import com.netflix.zeno.diff.DiffInstruction;
import com.netflix.zeno.diff.DiffOperation;
import com.netflix.zeno.diff.DiffReport;
import com.netflix.zeno.diff.TypeDiff;
import com.netflix.zeno.diff.TypeDiff.FieldDiff;
import com.netflix.zeno.diff.TypeDiff.ObjectDiffScore;
import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.genericobject.DiffHtmlGenerator;

public class StateEngineDiffStep implements Tasklet {

	private FastBlobStateEngine stateEngineFrom;
	private FastBlobStateEngine stateEngineTo;
	
	/** The file location for storing snapshots and deltas */
	private String serializedDataLocation;

	@Override
	public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
		
		File serializedDataLocationFile = new File(this.serializedDataLocation);
		File snapshotsLocationFile = new File(serializedDataLocationFile, SerializerConstants.SNAPSHOT_LOCATION);
		File deltaLocationFile = new File(serializedDataLocationFile, SerializerConstants.DELTA_LOCATION);		
		
		Arrays.sort(snapshotsLocationFile.listFiles());
		File snapshotFile = snapshotsLocationFile.listFiles()[snapshotsLocationFile.listFiles().length - 1];				
	    FastBlobReader snapshotReader = new FastBlobReader(stateEngineFrom);
	    snapshotReader.readSnapshot(new DataInputStream(new BufferedInputStream(new FileInputStream(snapshotFile))));

		Arrays.sort(deltaLocationFile.listFiles());
		File deltaFile = deltaLocationFile.listFiles()[deltaLocationFile.listFiles().length - 1];				
	    FastBlobReader deltaReader = new FastBlobReader(stateEngineTo);
	    deltaReader.readDelta(new DataInputStream(new BufferedInputStream(new FileInputStream(deltaFile))));
	    
		DiffInstruction instruction = getDiffInstruction();
		DiffOperation diffOperation = new DiffOperation(new RootSerializerFactory(), instruction);
	    DiffReport diffReport = diffOperation.performDiff(stateEngineFrom, stateEngineTo);
        System.out.println("Total Differences Between Matched Objects: " + diffReport.getTotalDiffs());
        System.out.println("Total Unmatched Objects: " + diffReport.getTotalExtra());
        
        /// get the differences for a single type
        TypeDiff<UserInfo> typeDiff = diffReport.getTypeDiff(UserInfo.class.getName());

        for (ObjectDiffScore<UserInfo> ods : typeDiff.getDiffObjects()) {
        	System.out.println("Object with differences : " + ((UserInfo)ods.getFrom()).getId());
        }
        
        /// iterate through all fields for that type
        for(FieldDiff<UserInfo> fieldDiff : typeDiff.getSortedFieldDifferencesDescending()) {
            String propertyName = fieldDiff.getPropertyPath().toString();
            int totalExamples = fieldDiff.getDiffScore().getTotalCount();
            int unmatchedExamples = fieldDiff.getDiffScore().getDiffCount();
            if (unmatchedExamples > 0) {
            	System.out.println(propertyName + ": " + unmatchedExamples + " / " + totalExamples + " were unmatched");
            }
        }     
        
        /// iterate over each of the different instances
        String name = "diffreport";
        int count = 0;
        for(ObjectDiffScore<UserInfo> objectDiff : typeDiff.getDiffObjects()) {
            DiffHtmlGenerator generator = new DiffHtmlGenerator(new RootSerializerFactory());
            File file = new File("/Users/regunath.balasubramanian/Documents/junk/" + name + count + ".html");
            file.createNewFile();
            FileWriter out = new FileWriter(file);
            out.append(generator.generateDiff(UserInfo.class.getName(), objectDiff.getFrom(),objectDiff.getTo()));
            out.flush();
            out.close();
            count += 1;
        }
        
		return RepeatStatus.FINISHED;
	}

	public DiffInstruction getDiffInstruction() {
		return new DiffInstruction(new TypeDiffInstruction<UserInfo>() {
			public String getSerializerName() {
				return UserInfo.class.getName();
			}
			public Object getKey(UserInfo userInfo) {
				return userInfo.getId();
			}
		});
	}

	/** Getter/Setter methods */
	
	public String getSerializedDataLocation() {
		return serializedDataLocation;
	}
	public FastBlobStateEngine getStateEngineFrom() {
		return stateEngineFrom;
	}
	public void setStateEngineFrom(FastBlobStateEngine stateEngineFrom) {
		this.stateEngineFrom = stateEngineFrom;
	}
	public FastBlobStateEngine getStateEngineTo() {
		return stateEngineTo;
	}
	public void setStateEngineTo(FastBlobStateEngine stateEngineTo) {
		this.stateEngineTo = stateEngineTo;
	}
	public void setSerializedDataLocation(String serializedDataLocation) {
		this.serializedDataLocation = serializedDataLocation;
	}

}
