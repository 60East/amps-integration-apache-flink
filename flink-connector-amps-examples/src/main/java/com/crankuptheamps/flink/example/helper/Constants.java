///////////////////////////////////////////////////////////////////////////
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

package com.crankuptheamps.flink.example.helper;

import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.util.ParameterTool;

/** Several constants involved in running the examples. */
public class Constants {
    public static class URI {
        /** The URI for the AMPS instance that runs using the config.xml configuration. */
        public static final String JSON = "tcp://localhost:9006/amps/json";
        /** The URI for the AMPS instance that runs using the amps1config.xml configuration. */
        public static final String JSON_1 = "tcp://localhost:9008/amps/json";
        /** The URI for the AMPS instance that runs using the amps2config.xml configuration. */
        public static final String JSON_2 = "tcp://localhost:9010/amps/json";
        /** The URI that uses SSL for the AMPS instance that runs the config.xml configuration. */
        public static final String S_JSON = "tcps://localhost:9020/amps/json";
    }

    public static class TOPIC {
        public static final String SOW_MESSAGE = "sow_message";
        public static final String SOW_MARKET_DATA = "market_data";
        public static final String ADHOC_MESSAGE = "messages";
        public static final String TXLOG_MARKET_DATA = "txlog_from_amps";
        public static final String TXLOG_FLINK = "txlog_from_flink";
        public static final String TXLOG_INC = "txlog_inc";
        public static final String TXLOG_CHECKPOINTING = "txlog_checkpointing";
        public static final String Q_WORK = "Work";
        public static final String Q_WORK_TO_DO = "WorkToDo";
        public static final String Q_MY_DATA = "MyData";
        public static final String Q_MY_DATA_TO_PROCESS = "MyDataToProcess";
        public static final String Q_FAILED_MY_DATA = "FailedMyData";
    }

    public static class ARGS {
        public static final String PUBLISH_MESSAGES_AMOUNT_OVERRIDE = "publishAmount";
        public static final String SPLIT_AMOUNT_OVERRIDE = "splitAmount";
        public static final String PARALLELISM_OVERRIDE = "parallelism";
    }

    /**
     * Checks if the root is a JobCancellationException.
     *
     * If it is, print that the job was cancelled.
     * If it is not, print the stack trace.
     */
    public static void checkForJobCancellationException(Throwable e) {
        Throwable cause = e;

        while (cause != null) {
            if (cause instanceof JobCancellationException && cause.getCause() == null) {
                System.out.println("Job was cancelled");
                return;
            }
            cause = cause.getCause();
        }

        e.printStackTrace();
    }

    /**
     * Gets the amount of messages to publish based on an argument or the given default value.
     */
    public static int getPublishAmount(String[] args, int defaultValue) {
        return getInt(args, ARGS.PUBLISH_MESSAGES_AMOUNT_OVERRIDE, defaultValue, 0);
    }
    
    /**
     * Gets the amount of splits to create based on an argument or the given default value.
     */
    public static int getSplitAmount(String[] args, int defaultValue) {
        return getInt(args, ARGS.SPLIT_AMOUNT_OVERRIDE, defaultValue, 0);
    }

    /**
     * Gets the parallelism to run the Flink job at based on an argument or the given default value.
     */
    public static int getParallelism(String[] args, int defaultValue) {
        return getInt(args, ARGS.PARALLELISM_OVERRIDE, defaultValue, 1);
    }

    /**
     * Gets an integer from the args.
     */
    private static int getInt(String[] args, String argument, int defaultValue, int min) {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        try {
            int num = parameters.getInt(argument, defaultValue);

            return num >= min ? num : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }
}

