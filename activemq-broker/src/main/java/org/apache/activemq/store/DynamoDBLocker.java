/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.AbstractLocker;
import org.apache.activemq.util.ServiceStopper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClientOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.CreateDynamoDBTableOptions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBLockClient;
import com.amazonaws.services.dynamodbv2.LockItem;
import com.amazonaws.services.dynamodbv2.AcquireLockOptions;
import com.amazonaws.services.dynamodbv2.model.LockTableDoesNotExistException;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

import javax.naming.ConfigurationException;

/**
 * Represents an exclusive lock on a database to avoid multiple brokers running
 * against the same logical database.
 *
 * @org.apache.xbean.XBean element="dynamodb-locker"
 */


public class DynamoDBLocker extends AbstractLocker {

    private String lockTableName;

    private static final Logger LOG = LoggerFactory.getLogger(DynamoDBLocker.class);

    private AmazonDynamoDBLockClient lockClient;
    private Optional<LockItem> lockItem = Optional.empty();

    public void setLockTableName(String lockTableName) {
        log("Lock table name configured to '" + lockTableName + "'.");
        this.lockTableName = lockTableName;
    }

    @Override
    public void doStart() throws Exception {
        boolean warned = false;

        while ((!isStopped()) && (!isStopping())) {

            if (!warned) {
                log("Attempting to obtain lock.");
            }

            lockItem = lockClient.tryAcquireLock(AcquireLockOptions.builder(getBrokerId()).build());

            if (lockItem.isPresent()) {
                log("Successfully obtained the lock!");
                break;
            }

            if (!warned) {
                log("Failed to obtain the lock. This instance is in standby mode.");
                warned = true;
            }
            log("Waiting " + (lockAcquireSleepInterval / 1000) + " seconds before attempting to obtain the lock again.");
            try {
                TimeUnit.MILLISECONDS.sleep(lockAcquireSleepInterval);
            } catch (InterruptedException e) {
            }
        }
        if (lockItem.isEmpty()) {
            throw new IOException("Attempt to obtain lock aborted due to shutdown");
        }
    }

    @Override
    public boolean keepAlive() {
        if (lockItem.isPresent()) {
            lockItem.get().sendHeartBeat();
            log("Sent heartbeat for keep alive.");
            return true;
        } else {
            log("Skipped sending heartbeat for keep alive since the lock is not present.");
            return false;
        }
    }

    @Override
    public void doStop(ServiceStopper stopper) throws Exception {
        if (lockItem.isPresent()) {
            lockClient.releaseLock(lockItem.get());
            log("Released the lock.");
        } else {
            log("Skipped releasing the lock since the lock is not present.");
        }
    }

    @Override
    public void configure(PersistenceAdapter persistenceAdapter) throws IOException {
        log("Configuring DynamoDBLocker.");

        if (lockTableName == null || toString().isBlank()) {
            throw new RuntimeException("lockTableName is not configured on " + DynamoDBLocker.class);
        }

        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard().build();

        lockClient = new AmazonDynamoDBLockClient(
                AmazonDynamoDBLockClientOptions.builder(dynamoDB, lockTableName)
                        .withTimeUnit(TimeUnit.SECONDS)
                        .withLeaseDuration(10L)
                        .withHeartbeatPeriod(5L)
                        .build()
        );

        try {
            lockClient.assertLockTableExists();
        } catch (LockTableDoesNotExistException e) {
            log("Creating the lock table.");
            AmazonDynamoDBLockClient.createLockTableInDynamoDB(CreateDynamoDBTableOptions
                    .builder(dynamoDB, new ProvisionedThroughput(1L, 1L), lockTableName)
                    .build()
            );
            log("Successfully created the lock table.");
        }
    }

    // NOTE: Special log format to make it visually easy to see log messages for this class.
    private void log(final String message) {
        LOG.info("(╯°□°)╯︵ ┻━┻ {}", message);
    }

    // TODO: Stub method to represent locking a specific broker.
    private String getBrokerId() {
        return "b-1234567890";
    }
}