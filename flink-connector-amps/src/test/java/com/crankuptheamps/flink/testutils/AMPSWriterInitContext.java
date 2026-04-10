////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2024-2026 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

package com.crankuptheamps.flink.testutils;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;

import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

public class AMPSWriterInitContext implements WriterInitContext {
    public final AMPSSinkWriterMetricGroup metricGroup = new AMPSSinkWriterMetricGroup(); 
    public final int parallelism;

    public AMPSWriterInitContext() {
        this.parallelism = 1;
    }

    public AMPSWriterInitContext(int parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return null;
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MailboxExecutor getMailboxExecutor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ProcessingTimeService getProcessingTimeService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserCodeClassLoader getUserCodeClassLoader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isObjectReuseEnabled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SinkWriterMetricGroup metricGroup() {
        return metricGroup;
    }

    @Override
    public JobInfo getJobInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return new TaskInfo() {
            @Override
            public String getAllocationIDAsString() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getAttemptNumber() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getIndexOfThisSubtask() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getMaxNumberOfParallelSubtasks() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getNumberOfParallelSubtasks() {
                return parallelism;
            }

            @Override
            public String getTaskName() {
                throw new UnsupportedOperationException();
            }

            @Override
            public String getTaskNameWithSubtasks() {
                throw new UnsupportedOperationException();
            }
        };
    }
}

