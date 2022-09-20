package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

    private final HeapFile heapFile;
    private final TransactionId tid;

    private int pageNo = -1;
    private Iterator<Tuple> currTupleIter;

    private boolean hasNextPage() {
        return pageNo >= 0 && pageNo < heapFile.numPages();
    }

    private Iterator<Tuple> NextTupleIter() throws DbException {
        try {
            PageId pid = new HeapPageId(heapFile.getId(), pageNo++);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        } catch (Exception e) {
            throw new DbException("No page for current heapfile");
        }
    }

    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open() throws DbException, TransactionAbortedException {
        pageNo++;
        currTupleIter = null;
    }

    /**
     * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        // current tuple's iterator has next
        if (currTupleIter != null && currTupleIter.hasNext()) {
            return true;
        } else if (hasNextPage()) {
            // if it has next page, get next page's iterator
            currTupleIter = NextTupleIter();
            return currTupleIter.hasNext();
        } else {
            return false;
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return currTupleIter.next();
        }
        throw new NoSuchElementException();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }


    @Override
    public void close() {
        pageNo = -1;
        currTupleIter = null;
    }
}
