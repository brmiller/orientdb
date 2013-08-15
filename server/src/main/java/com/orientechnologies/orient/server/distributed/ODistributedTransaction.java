/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * Distributed transaction that checks if the record relies to the current node
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODistributedTransaction implements OTransaction {
  protected ODistributedStorage storage;
  protected OTransaction        wrapped;

  public ODistributedTransaction(final ODistributedStorage iStorage, final OTransaction iWrapped) {
    storage = iStorage;
    wrapped = iWrapped;
  }

  @Override
  public void onBeforeCommitEntry(final ORecordOperation txEntry) {
    if (!storage.getDistributedManager().isLocalNodeMaster(storage.getName(), null, txEntry.getRecord()))
      throw new OTransactionException("The master node for the involved record " + txEntry + " is not the current node.");
  }

  public void begin() {
    wrapped.begin();
  }

  public void commit() {
    wrapped.commit();
  }

  public void rollback() {
    wrapped.rollback();
  }

  public ODatabaseRecordTx getDatabase() {
    return wrapped.getDatabase();
  }

  public void clearRecordEntries() {
    wrapped.clearRecordEntries();
  }

  public ORecordInternal<?> loadRecord(ORID iRid, ORecordInternal<?> iRecord, String iFetchPlan, boolean ignoreCache,
      boolean loadTombstone) {
    return wrapped.loadRecord(iRid, iRecord, iFetchPlan, ignoreCache, loadTombstone);
  }

  public boolean updateReplica(ORecordInternal<?> iRecord) {
    return wrapped.updateReplica(iRecord);
  }

  public void saveRecord(ORecordInternal<?> iContent, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    wrapped.saveRecord(iContent, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  public void deleteRecord(ORecordInternal<?> iRecord, OPERATION_MODE iMode) {
    wrapped.deleteRecord(iRecord, iMode);
  }

  public int getId() {
    return wrapped.getId();
  }

  public TXSTATUS getStatus() {
    return wrapped.getStatus();
  }

  public Iterable<? extends ORecordOperation> getCurrentRecordEntries() {
    return wrapped.getCurrentRecordEntries();
  }

  public Iterable<? extends ORecordOperation> getAllRecordEntries() {
    return wrapped.getAllRecordEntries();
  }

  public List<ORecordOperation> getRecordEntriesByClass(String iClassName) {
    return wrapped.getRecordEntriesByClass(iClassName);
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds) {
    return wrapped.getNewRecordEntriesByClusterIds(iIds);
  }

  public ORecordInternal<?> getRecord(ORID iRid) {
    return wrapped.getRecord(iRid);
  }

  public ORecordOperation getRecordEntry(ORID rid) {
    return wrapped.getRecordEntry(rid);
  }

  public List<String> getInvolvedIndexes() {
    return wrapped.getInvolvedIndexes();
  }

  public ODocument getIndexChanges() {
    return wrapped.getIndexChanges();
  }

  public void addIndexEntry(OIndex<?> delegate, String iIndexName, OPERATION iStatus, Object iKey, OIdentifiable iValue) {
    wrapped.addIndexEntry(delegate, iIndexName, iStatus, iKey, iValue);
  }

  public void clearIndexEntries() {
    wrapped.clearIndexEntries();
  }

  public OTransactionIndexChanges getIndexChanges(String iName) {
    return wrapped.getIndexChanges(iName);
  }

  public boolean isActive() {
    return wrapped.isActive();
  }

  public boolean isUsingLog() {
    return wrapped.isUsingLog();
  }

  public void setUsingLog(boolean useLog) {
    wrapped.setUsingLog(useLog);
  }

  public void close() {
    wrapped.close();
  }

  public void updateIdentityAfterCommit(ORID oldRid, ORID newRid) {
    wrapped.updateIdentityAfterCommit(oldRid, newRid);
  }
}
