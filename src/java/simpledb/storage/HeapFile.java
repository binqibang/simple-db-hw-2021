package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile rf = new RandomAccessFile(file, "r");
            byte[] bytes = new byte[BufferPool.getPageSize()];
            // set the start pointer of file to read
            rf.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            // read file's content into bytes array
            if (rf.read(bytes) == -1) {
                throw new IllegalArgumentException(String.format("No more data for ( table %d, page %d)", pid.getTableId(), pid.getPageNumber()));
            }
            rf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) {
        try {
            RandomAccessFile rf = new RandomAccessFile(file, "rw");
            rf.seek((long) BufferPool.getPageSize() * page.getId().getPageNumber());
            rf.write(page.getPageData());
            rf.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        for (int pageNo = 0; pageNo < numPages(); pageNo++) {
            HeapPageId pid = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }
        }
        // if all pages are full
        byte[] pageData = HeapPage.createEmptyPageData();
        HeapPageId newPid = new HeapPageId(this.getId(), numPages());
        writePage(new HeapPage(newPid, pageData));
        HeapPage appendPage = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
        appendPage.insertTuple(t);
        pages.add(appendPage);
        return pages;
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        List<Page> pages = new ArrayList<>();
        if (!tupleDesc.equals(t.getTupleDesc())) {
            throw new DbException("Tupledesc mismatch");
        }
        if (this.getId() != t.getRecordId().getPageId().getTableId()) {
            throw new DbException("This tuple is not in this file");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> currTupleIter = null;
        HeapPage currPage = null;

        final TransactionId tid;
        final HeapFile heapFile;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            this.heapFile = hf;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId firstPageId = new HeapPageId(getId(), 0);
            currPage = (HeapPage) Database.getBufferPool().getPage(tid, firstPageId, Permissions.READ_ONLY);
            currTupleIter = currPage.iterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            // currPage has no more tuple to iterate, set null
            if (currTupleIter != null && !currTupleIter.hasNext()) {
                currTupleIter = null;
            }
            while (currTupleIter == null && currPage != null) {
                HeapPageId nextPid = null;
                HeapPageId currPid = currPage.pid;
                if (currPid.getPageNumber() < numPages() - 1) {
                    nextPid = new HeapPageId(getId(), currPid.getPageNumber() + 1);
                }
                if (nextPid == null) {
                    currPage = null;
                } else {
                    // fetch next page
                    currPage = (HeapPage) Database.getBufferPool().getPage(tid, nextPid, Permissions.READ_ONLY);
                    currTupleIter = currPage.iterator();
                    // page has not tuple
                    if (!currTupleIter.hasNext()) {
                        currTupleIter = null;
                    }
                }
            }
            if (currTupleIter == null) {
                return null;
            }
            return currTupleIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            currTupleIter = null;
            currPage = null;
        }
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

