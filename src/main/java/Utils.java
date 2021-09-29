package main.java;/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.kafka.common.protocol.types.Field;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;
import java.util.UUID;

public class Utils {
    private static final Random RANDOM = new Random();

    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }

    /**
     * Generates a blob containing a UTF-8 string. The string begins with the
     * sequence number in decimal notation, followed by a space, followed by
     * padding.
     *
     * @param sequenceNumber
     *            The sequence number to place at the beginning of the record
     *            data.
     * @param totalLen
     *            Total length of the data. After the sequence number, padding
     *            is added until this length is reached.
     * @return ByteBuffer containing the blob
     */
    public static ByteBuffer generateData(long sequenceNumber, int totalLen) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(sequenceNumber));
        sb.append(" ");
        while (sb.length() < totalLen) {
            sb.append("a");
        }
        try {
            return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String[] GetUsersFromFile(){
        String[] usernames;
        StringBuilder sb = new StringBuilder();

        File file = new File("src/main/java/list-users.txt");
        try {
            Scanner scanner = new Scanner(file);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                if(line.contains("Username")){
                    line = line.replaceAll("\"", "").replace(",", "").replace("Username: ", "");
                    sb.append(line + " ");
                }
            }
            usernames = sb.toString().split(" ");
            System.out.println(usernames[0]);
            System.out.println(usernames[1]);
            System.out.println(usernames[2]);
            System.out.println(usernames[3]);
            System.out.println(usernames[4]);

            return usernames;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String[] GetARNsFromFile(){
        String[] arns;
        StringBuilder sb = new StringBuilder();

        File file = new File("src/main/java/list-users.txt");
        try {
            Scanner scanner = new Scanner(file);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                if(line.contains("Arn")){
                    line = line.replaceAll("\"", "").replace("Arn: ", "");
                    sb.append(line + " ");
                }
            }
            arns = sb.toString().split(" ");
            System.out.println(arns.length);
            return arns;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String[] SetUsers(){
        String[] accountUsername;
        try {
            BufferedReader reader = new BufferedReader(new FileReader("src/main/java/users.txt"));
            String users = reader.readLine();

            accountUsername = users.split("\\s+");
            System.out.println(accountUsername[0]);
            return accountUsername;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String[] SetARNs(){
        String[] accountARNs;
        try {
            BufferedReader reader = new BufferedReader(new FileReader("src/main/java/arn.txt"));
            String arns = reader.readLine();

            accountARNs = arns.split("\\s+");
            System.out.println(accountARNs[0]);
            return accountARNs;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String generateCTR(long input, String[] accountARN, String[] accountUsername) {

        int accountInfoRandom = new Random().nextInt(accountARN.length);

        String ARN = accountARN[accountInfoRandom];
        String Username = accountUsername[accountInfoRandom];
        int holdDuration = new Random().nextInt(32);
        UUID randomGUID = UUID.randomUUID();

        String ctr = "{  \"AWSAccountId\": \"927185244192\",  \"AWSContactTraceRecordFormatVersion\": \"2017-03-10\",  \"Agent\": {    \"ARN\": \"" + ARN + "\",    \"AfterContactWorkDuration\": 5,    \"AfterContactWorkEndTimestamp\": \"2021-08-16T08:27:48Z\",    \"AfterContactWorkStartTimestamp\": \"2021-08-16T08:27:43Z\",    \"AgentInteractionDuration\": 54,    \"ConnectedToAgentTimestamp\": \"2021-08-16T08:26:26Z\",    \"CustomerHoldDuration\": " + holdDuration + ",    \"HierarchyGroups\": {      \"Level1\": {        \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/agent-group/a01e6fb3-6e99-47fc-a165-c76ac344dd4d\",        \"GroupName\": \"SVP1\"      },      \"Level2\": {        \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/agent-group/3e6e0039-ef7c-4dc6-baa0-f5f567ac066a\",        \"GroupName\": \"DivisionVP1\"      },      \"Level3\": {        \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/agent-group/6fbe6b75-2b7f-4e1e-9310-84479a7e320c\",        \"GroupName\": \"Director1\"      },      \"Level4\": {        \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/agent-group/bae34b46-7b90-4a41-b248-b10fd231bbb2\",        \"GroupName\": \"Team2\"      },      \"Level5\": null    },    \"LongestHoldDuration\": " + holdDuration + ",    \"NumberOfHolds\": 1,    \"RoutingProfile\": {      \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/routing-profile/0b8520f1-685d-4502-9e11-23f4e8e0926f\",      \"Name\": \"AQM Routing Profile\"    },    \"Username\": \"" + Username + "\"  },  \"AgentConnectionAttempts\": 1,  \"Attributes\": {    \"key\": \"value" + input + "\"  },  \"Channel\": \"VOICE\",  \"ConnectedToSystemTimestamp\": \"2021-08-16T08:26:20Z\",  \"ContactDetails\": {},  \"ContactId\": \"" + randomGUID + "\",  \"CustomerEndpoint\": {    \"Address\": \"+918043400593\",    \"Type\": \"TELEPHONE_NUMBER\"  },  \"DisconnectReason\": \"OTHER\",  \"DisconnectTimestamp\": \"2021-08-16T08:27:28Z\",  \"InitialContactId\": null,  \"InitiationMethod\": \"INBOUND\",  \"InitiationTimestamp\": \"2021-08-16T08:26:20Z\",  \"InstanceARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048\",  \"LastUpdateTimestamp\": \"2021-08-16T08:28:55Z\",  \"MediaStreams\": [    {      \"Type\": \"AUDIO\"    }  ],  \"NextContactId\": \"96503872-519f-42bf-94ac-123159285502\",  \"PreviousContactId\": null,  \"Queue\": {    \"ARN\": \"arn:aws:connect:us-east-1:927185244192:instance/57faaaa3-4c4a-47ee-ab43-dc64748c0048/queue/cd29fbba-be8f-4c7f-be8f-4e1247046116\",    \"DequeueTimestamp\": \"2021-08-16T08:26:26Z\",    \"Duration\": 4,    \"EnqueueTimestamp\": \"2021-08-16T08:26:22Z\",    \"Name\": \"AQMQueue\"  },  \"Recording\": {    \"DeletionReason\": null,    \"Location\": \"connect-bd6a685ba656/connect/robdemo/recordings/2021/08/16/08acbb66-ea0c-406b-9583-21ed578f3e67_20210816T08:26_UTC.wav\",    \"Status\": \"AVAILABLE\",    \"Type\": \"AUDIO\"  },  \"Recordings\": [    {      \"DeletionReason\": null,      \"FragmentStartNumber\": null,      \"FragmentStopNumber\": null,      \"Location\": \"connect-bd6a685ba656/connect/robdemo/recordings/2021/08/16/08acbb66-ea0c-406b-9583-21ed578f3e67_20210816T08:26_UTC.wav\",      \"MediaStreamType\": \"AUDIO\",      \"ParticipantType\": null,      \"StartTimestamp\": null,      \"Status\": \"AVAILABLE\",      \"StopTimestamp\": null,      \"StorageType\": \"S3\"    }  ],  \"References\": [],  \"SystemEndpoint\": {    \"Address\": \"+17165599031\",    \"Type\": \"TELEPHONE_NUMBER\"  },  \"TransferCompletedTimestamp\": null,  \"TransferredToEndpoint\": null}\n";

        return ctr;
    }
}