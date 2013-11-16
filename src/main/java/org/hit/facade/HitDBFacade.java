/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.hit.facade;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.hit.communicator.Communicator;
import org.hit.communicator.CommunicatorException;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Persistable;
import org.hit.db.model.Query;
import org.hit.db.model.Row;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.RangeMutation;
import org.hit.db.model.mutations.SingleKeyMutation;
import org.hit.db.model.query.RewritableQuery;
import org.hit.db.partitioner.DistributedHashTable;
import org.hit.db.partitioner.Partitioner;
import org.hit.db.partitioner.TablePartitionInfo;
import org.hit.db.query.merger.QueryMerger;
import org.hit.db.query.merger.SimpleQueryMerger;
import org.hit.db.query.operators.QueryBuilder;
import org.hit.db.query.operators.QueryBuildingException;
import org.hit.db.query.parser.HitSQLLexer;
import org.hit.db.query.parser.HitSQLParser;
import org.hit.db.query.parser.HitSQLTree;
import org.hit.di.HitFacadeModule;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DBOperationSuccessMessage;
import org.hit.messages.DistributedDBOperationMessage;
import org.hit.messages.FacadeInitRequest;
import org.hit.messages.FacadeInitResponse;
import org.hit.registry.RegistryService;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.util.Pair;
import org.hit.util.Range;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Defines the contract for a class that acts as a client to the database.
 * Primarily this class reads the commands/statements from the command line
 * and after parsing the same sends it down to the servers.
 *
 * @author Balraja Subbiah
 */
public class HitDBFacade
{
    /**
     * A simple class that wraps the functionality of handling the
     * response from the server.
     */
    private class CommunicatorResponseHandlerTask implements Runnable
    {
        private final Message myMessage;

        /**
         * CTOR
         */
        public CommunicatorResponseHandlerTask(Message message)
        {
            super();
            myMessage = message;
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("unchecked")
        @Override
        public void run()
        {
            if (myMessage instanceof FacadeInitResponse) {
                FacadeInitResponse fir = (FacadeInitResponse) myMessage;
                myTablePartitionInfo.merge(fir.getPartitionInfo());
                myIsInitialized.set(true);
            }
            else if (myMessage instanceof CreateTableResponseMessage) {

                final CreateTableResponseMessage createTableResponse =
                    (CreateTableResponseMessage) myMessage;

                SettableFuture<CreateTableResponseMessage>
                    tableCreationFuture =
                        myTableCreationFutureMap.get(
                            createTableResponse.getTableName());

                tableCreationFuture.set(createTableResponse);
            }
            else if (myMessage instanceof DBOperationSuccessMessage) {
                final DBOperationSuccessMessage dbOperationSuccessMessage =
                   (DBOperationSuccessMessage) myMessage;

                final Long id =
                    Long.valueOf(
                        dbOperationSuccessMessage.getSequenceNumber());

                final SettableFuture<DBOperationResponse> future  =
                    myMutationIDToFutureMap.get(id);

                if (future != null) {
                    future.set(new DBOperationResponse(
                        dbOperationSuccessMessage.getResult()));
                }
                else {
                    List<SettableFuture<Pair<NodeID, Collection<Row>>>>
                        queryFutures = 
                            myQueryToMergableFuturesMap.get(id);

                    if (queryFutures != null & !queryFutures.isEmpty()) {
                        queryFutures.remove(0).set(new Pair<>(
                            dbOperationSuccessMessage.getSenderId(),
                            (Collection<Row>)
                                dbOperationSuccessMessage.getResult()));
                    }
                    else {
                        LOG.severe("Received "
                                   + dbOperationSuccessMessage.getResult()
                                   + " for an operation with seq number "
                                   + id
                                   + " But corresponding future is not found");
                    }
                }
            }
            else if (myMessage instanceof DBOperationFailureMessage) {
                final DBOperationFailureMessage dbOperationFailure =
                    (DBOperationFailureMessage) myMessage;

                final Long id =
                    Long.valueOf(dbOperationFailure.getSequenceNumber());

                final SettableFuture<DBOperationResponse> future  =
                    myMutationIDToFutureMap.get(id);

                if (future != null) {
                    future.setException(
                        dbOperationFailure.getException());
                }
                else {
                    List<SettableFuture<Pair<NodeID, Collection<Row>>>>
                        queryFutures = myQueryToMergableFuturesMap.get(id);

                    if (queryFutures != null && !queryFutures.isEmpty()) {
                        queryFutures.remove(0).setException(
                            dbOperationFailure.getException());
                    }
                    else {
                        LOG.severe("Received " + dbOperationFailure.getMessage()
                                   + " for an operation with seq number "
                                   + dbOperationFailure.getSequenceNumber()
                                   + " But corresponding future is not found");
                    }
                }
            }
        }
    }
    
    private class RangeQueryResponserHandler
        implements FutureCallback<Pair<NodeID, Collection<Row>>>
    {
        private final SettableFuture<QueryResponse> myClientFuture;

        private final Long myOperationId;

        private final QueryMerger myQueryMerger;

        private final Set<NodeID> myServerNodes;

        /**
         * CTOR
         */
        public RangeQueryResponserHandler(
            Long operationId,
            Set<NodeID> serverNodes,
            QueryMerger queryMerger,
            SettableFuture<QueryResponse> clientFuture)
        {
            super();
            myOperationId = operationId;
            myServerNodes = serverNodes;
            myQueryMerger = queryMerger;
            myClientFuture = clientFuture;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            myClientFuture.setException(exception);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Pair<NodeID, Collection<Row>> result)
        {
            myServerNodes.remove(result.getFirst());
            myQueryMerger.addPartialResult(result.getSecond());
            if (myServerNodes.isEmpty()) {
                myClientFuture.set(new QueryResponse(
                    myQueryMerger.getMergedResult()));
                myQueryToMergableFuturesMap.remove(myOperationId);
            }
        }
    }
    
    private class PointQueryResponserHandler
        implements FutureCallback<Pair<NodeID, Collection<Row>>>
    {
        private final SettableFuture<QueryResponse> myClientFuture;
    
        /**
         * CTOR
         */
        public PointQueryResponserHandler(
            SettableFuture<QueryResponse> clientFuture)
        {
            super();
            myClientFuture = clientFuture;
        }
    
        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            myClientFuture.setException(exception);
        }
    
        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Pair<NodeID, Collection<Row>> result)
        {
            myClientFuture.set(new QueryResponse(result.getSecond()));
        }
    }

    /**
     * A helper class that wraps the task of sending mutation to the
     * server.
     */
    private class SubmitDBOperationTask implements Runnable
    {
        private final NodeID myNodeID;

        private final DBOperation myOperation;

        private final long mySequenceNumber;

        private final SettableFuture<DBOperationResponse> mySettableFuture;

        /**
         * CTOR
         */
        public SubmitDBOperationTask(NodeID                              nodeId,
                                     DBOperation                         operation,
                                     SettableFuture<DBOperationResponse> future,
                                     long seqNumber)
        {
            myNodeID = nodeId;
            myOperation = operation;
            mySettableFuture = future;
            mySequenceNumber = seqNumber;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for performing the db operation " +
                            myOperation);

            myMutationIDToFutureMap.put(mySequenceNumber, mySettableFuture);

            try {
                myCommunicator.sendTo(myNodeID,
                                      new DBOperationMessage(myClientID,
                                                             mySequenceNumber,
                                                             myOperation));
            }
            catch (CommunicatorException e) {
                mySettableFuture.setException(e);
            }
        }
    }
    
    /**
     * A helper class that wraps the task of sending mutation to the
     * server.
     */
    private class SubmitDistributedTransactionTask implements Runnable
    {
        private final NodeID myNodeID;

        private final Map<NodeID, DBOperation> myOperations;

        private final long mySequenceNumber;

        private final SettableFuture<DBOperationResponse> mySettableFuture;

        /**
         * CTOR
         */
        public SubmitDistributedTransactionTask(
            NodeID                              nodeId,
            Map<NodeID, DBOperation>            operations,
            SettableFuture<DBOperationResponse> future,
            long                                seqNumber)
        {
            myNodeID = nodeId;
            myOperations = operations;
            mySettableFuture = future;
            mySequenceNumber = seqNumber;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for performing the db operation " +
                            myOperations);

            myMutationIDToFutureMap.put(mySequenceNumber, mySettableFuture);

            try {
                myCommunicator.sendTo(myNodeID,
                                      new DistributedDBOperationMessage(
                                          myClientID, 
                                          mySequenceNumber, 
                                          myOperations));
            }
            catch (CommunicatorException e) {
                mySettableFuture.setException(e);
            }
        }
    }

    private class SubmitQueryTask implements Runnable
    {
        private final Query myQuery;

        private final long mySequenceNumber;
        
        private final 
            FutureCallback<Pair<NodeID, Collection<Row>>> myResultCallback;
        
        private final NodeID myServer;
        
        /**
         * CTOR
         */
        public SubmitQueryTask(Query query,
                               long sequenceNumber,
                               FutureCallback<Pair<NodeID, Collection<Row>>>
                                   resultCallback,
                               NodeID server)
        {
            super();
            myQuery = query;
            mySequenceNumber = sequenceNumber;
            myResultCallback = resultCallback;
            myServer = server;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            List<SettableFuture<Pair<NodeID,Collection<Row>>>> resultFutures =
                myQueryToMergableFuturesMap.get(mySequenceNumber);
            
            if (resultFutures == null) {
                resultFutures = new ArrayList<>();
                myQueryToMergableFuturesMap.put(mySequenceNumber,
                                                resultFutures);
            }
            SettableFuture<Pair<NodeID,Collection<Row>>> resultFuture =
                SettableFuture.create();
            Futures.addCallback(resultFuture, myResultCallback);

            try {
                myCommunicator.sendTo(
                    myServer,
                    new DBOperationMessage(myClientID,
                                           mySequenceNumber,
                                           myQuery));
            }
            catch (CommunicatorException e) {
                myResultCallback.onFailure(e);
            }
        }
    }

    /**
     * A helper class that wraps the task of sending mutation to the
     * server.
     */
    private class SubmitTableCreationTask implements Runnable
    {
        private final SettableFuture<TableCreationResponse> myFuture;

        private final Schema mySchema;

        /**
         * CTOR
         */
        public SubmitTableCreationTask(
            SettableFuture<TableCreationResponse> future,
            Schema schema)
        {
            super();
            myFuture = future;
            mySchema = schema;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for creating  table with schema " +
                     mySchema);

            final TableCreationResponseHandler tableCreationHandler =
                new TableCreationResponseHandler(myFuture);
            final SettableFuture<CreateTableResponseMessage> tableCreationFuture
                = SettableFuture.create();
            Futures.addCallback(tableCreationFuture, tableCreationHandler);
            myTableCreationFutureMap.put(mySchema.getTableName(),
                                         tableCreationFuture);

            final CreateTableMessage message =
                new CreateTableMessage(myClientID, mySchema);
            try {
                myCommunicator.sendTo(myRegistryService.getMasterNode(),
                                      message);
            }
            catch (CommunicatorException e) {
                tableCreationFuture.setException(e);
            }
        }
    }

    private class TableCreationResponseHandler
        implements FutureCallback<CreateTableResponseMessage>
    {
        private final SettableFuture<TableCreationResponse> myClientFuture;

        /**
         * CTOR
         */
        public TableCreationResponseHandler(
            SettableFuture<TableCreationResponse> clientFuture)
        {
            myClientFuture = clientFuture;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable t)
        {
            myClientFuture.setException(t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(CreateTableResponseMessage response)
        {
            if (response.getErrorMessage() != null) {
                LOG.info("Received the error " + response.getErrorMessage());
                myClientFuture.setException(
                    new RuntimeException(response.getErrorMessage()));
            }
            else {
                myTablePartitionInfo.addPartitionInfo(
                    response.getTableName(), response.getPartitioner());

                myClientFuture.set(
                    new TableCreationResponse(response.getTableName()));
            }

        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDBFacade.class);

    private final NodeID myClientID;

    private final Communicator myCommunicator;

    private final ListeningExecutorService myExecutorService;

    private final AtomicBoolean myIsInitialized;

    private final Map<Long, SettableFuture<DBOperationResponse>>
        myMutationIDToFutureMap;

    private final AtomicLong myOperationsCount;

    private final Map<Long, List<SettableFuture<Pair<NodeID, Collection<Row>>>>>
        myQueryToMergableFuturesMap;

    private final RegistryService myRegistryService;

    private final TablePartitionInfo myTablePartitionInfo;

    private final Map<String, SettableFuture<CreateTableResponseMessage>>
        myTableCreationFutureMap;

    /**
     * CTOR
     */
    public HitDBFacade()
    {
        final Injector injector = Guice.createInjector(new HitFacadeModule());
        myCommunicator = injector.getInstance(Communicator.class);
        myRegistryService = injector.getInstance(RegistryService.class);
        myClientID = injector.getInstance(NodeID.class);
        myTablePartitionInfo = new TablePartitionInfo();
        myMutationIDToFutureMap = new HashMap<>();
        myOperationsCount = new AtomicLong(0L);
        myTableCreationFutureMap = new HashMap<>();
        myQueryToMergableFuturesMap = new HashMap<>();

        myExecutorService =
            MoreExecutors.listeningDecorator(
                Executors.newSingleThreadExecutor(
                    new NamedThreadFactory(HitDBFacade.class)));

        myIsInitialized = new AtomicBoolean(false);

    }

    /**
     * Applies the {@link RangeMutation} to the database. However it expects
     * that the mutation should be applicable to a single node.
     */
    public <K extends Comparable<K>, P extends Persistable<K>>
        ListenableFuture<DBOperationResponse> apply(RangeMutation<K,P> mutation)
    {
        Partitioner<K,?> partitions = 
            myTablePartitionInfo.lookup(mutation.getTableName());

        if (partitions == null) {
            return null;
        }

        final Map<NodeID, ?> split =
            partitions.lookupNodes(mutation.getKeyRange());

        final SettableFuture<DBOperationResponse> futureResponse =
            SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();

        if (split.size() == 1) {
            // The range falls within an ambit of single server
            // Hence send it to that directly.
            myExecutorService.submit(new SubmitDBOperationTask(
                split.keySet().iterator().next(),
                mutation,
                futureResponse,
                id));
        }
        else {
           
            Map<NodeID, DBOperation> nodeToOperationMap = new HashMap<>();
            for (NodeID key : split.keySet()) {
                nodeToOperationMap.put(key, mutation);
            }
            
            myExecutorService.submit(new SubmitDistributedTransactionTask(
                split.keySet().iterator().next(),
                nodeToOperationMap,
                futureResponse,
                id));
        }

        return futureResponse;
    }

    /**
     * Applies the mutation to the database.
     */
    public <K extends Comparable<K>>
        ListenableFuture<DBOperationResponse> apply(
            SingleKeyMutation<K> mutation)
    {
        Partitioner<K,?> partitions =
            myTablePartitionInfo.lookup(mutation.getTableName());

        if (partitions == null) {
            return null;
        }

        final NodeID serverNode = partitions.lookupNode(mutation.getKey());
        final SettableFuture<DBOperationResponse> futureResponse =
            SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();
        myExecutorService.submit(new SubmitDBOperationTask(
            serverNode, mutation, futureResponse, id));
        return futureResponse;
    }

    /**
     * Creates a new table in the database with the given <code>Schema</code>
     */
    public ListenableFuture<TableCreationResponse> createTable(Schema schema)
    {
        final SettableFuture<TableCreationResponse> clientFuture =
            SettableFuture.create();

        myExecutorService.submit(new SubmitTableCreationTask(
            clientFuture, schema));

        return clientFuture;
    }

    /** Returns true if the facade is initalized */
    public boolean isInitialized()
    {
        return myIsInitialized.get();
    }

    /** Returns list of tables known to this database server */
    public Set<String> listTables()
    {
        return myTablePartitionInfo.getTableNames();
    }

    /**
     * A helper method to query the database.
     */
    public <K extends Comparable<K>> ListenableFuture<QueryResponse> 
        executePointQuery(Query query, String tableName, K queriedKey)
    {
        SettableFuture<QueryResponse> queryResponse = SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();
        Partitioner<K, ?>  partitioner = 
            myTablePartitionInfo.lookup(tableName);
        NodeID serverNode = partitioner.lookupNode(queriedKey);
        
        myExecutorService.submit(
            new SubmitQueryTask(query, 
                                id,
                                new PointQueryResponserHandler(queryResponse),
                                serverNode));
        return queryResponse;
    }
    
    /**
     * A helper method to query the database.
     */
    public <K extends Comparable<K>> ListenableFuture<QueryResponse> 
        executeRangeQuery(
            RewritableQuery query, String tableName, Range<K> range)
    {
        SettableFuture<QueryResponse> queryResponse = SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();
        Partitioner<K, ?>  partitioner = 
            myTablePartitionInfo.lookup(tableName);
        if (partitioner instanceof DistributedHashTable) {
            throw new RuntimeException("We cannot execute range queries"
                + " over hashed tables");
        }
        else {
            @SuppressWarnings("unchecked")
            Partitioner<K, K> linearPartitioner = 
                (Partitioner<K, K>) partitioner;
            final Map<NodeID, Range<K>> split =
                linearPartitioner.lookupNodes(range);
            FutureCallback<Pair<NodeID, Collection<Row>>> callback = 
                new RangeQueryResponserHandler(
                    id,
                    new HashSet<>(split.keySet()),
                    query.getQueryMerger(),
                    queryResponse);
            for (Map.Entry<NodeID, Range<K>> entry : split.entrySet()) {
                RewritableQuery nodeQuery = query.cloneQuery();
                nodeQuery.updateRange(entry.getValue());
                
                myExecutorService.submit(
                    new SubmitQueryTask(nodeQuery,
                                        id, 
                                        callback,
                                        entry.getKey()));
            }
            
        }
        return queryResponse;
    }
    
    /**
     * A helper method to query the database.
     */
    public ListenableFuture<QueryResponse> queryDB(Query query)
    {
        SettableFuture<QueryResponse> queryResponse = SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();
        FutureCallback<Pair<NodeID, Collection<Row>>> callback = 
            new RangeQueryResponserHandler(
                id,
                new HashSet<>(myRegistryService.getServerNodes()),
                new SimpleQueryMerger(),
                queryResponse);
        
        for (NodeID server : myRegistryService.getServerNodes()) {
            
            myExecutorService.submit(
                new SubmitQueryTask(query,
                                    id, 
                                    callback,
                                    server));
        }
        return queryResponse;
    }

    /**
     * A helper method to query the database. The query is executed 
     * across nodes 
     */
    public ListenableFuture<QueryResponse> queryDB(String query)
        throws QueryBuildingException, RecognitionException
    {
        ANTLRStringStream fs = new ANTLRStringStream(query);
        HitSQLLexer lex = new HitSQLLexer(fs);
        TokenRewriteStream tokens = new TokenRewriteStream(lex);
        HitSQLParser parser = new HitSQLParser(tokens);

        HitSQLParser.select_statement_return result =
            parser.select_statement();

        CommonTree t = (CommonTree) result.getTree();
        CommonTreeNodeStream nodeStream = new CommonTreeNodeStream(t);
        HitSQLTree tree = new HitSQLTree(nodeStream);
        tree.select_statement();

        QueryBuilder builder = new QueryBuilder(tree.getQueryAttributes());
        return queryDB(builder.buildQuery(false).getFirst());
    }

    /**
     * {@inheritDoc}
     */
    public void start()
    {
        while(!myRegistryService.isUp()) {
            // Wait till we connect to registry/zookeeper.
        }

        LOG.info("Connection to the registry " +
                 "service successfully established");

        try {
            myCommunicator.start();
        }
        catch (CommunicatorException e1) {
            throw new RuntimeException(e1);
        }

        LOG.info("Messaging service got started successfully");

        myCommunicator.addMessageHandler(
             new MessageHandler() {
                 @Override
                 public void handle(Message message)
                 {
                     myExecutorService.execute(
                         new CommunicatorResponseHandlerTask(message));
                 }
             }
        );

        LOG.info("Facade successfully started");
        NodeID master = myRegistryService.getMasterNode();
        try {
            myCommunicator.sendTo(master, new FacadeInitRequest(myClientID));
        }
        catch (CommunicatorException e) {
            throw new RuntimeException(e);
        }

        while (!myIsInitialized.get()) {
            // Loop till the facade is initialized.
        }
    }

    /**
     * {@inheritDoc}
     */
    public void stop()
    {
        myExecutorService.shutdown();
        LOG.info("Facade successfully stopped");
    }
}
