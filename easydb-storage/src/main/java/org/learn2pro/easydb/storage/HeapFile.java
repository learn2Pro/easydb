package org.learn2pro.easydb.storage;

import com.google.common.base.Preconditions;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular order. Tuples are
 * stored on pages, each of which is a fixed size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    /**
     * the file to store
     */
    private File file;
    /**
     * the file  struct
     */
    private TupleDesc struct;
    /**
     * the unique id for file
     */
    private int tableId;
    /**
     * the page size
     */
    private int pageSize;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.struct = td;
        this.tableId = f.getAbsoluteFile().hashCode();
        this.pageSize =
                (int) (f.length() / BufferPool.getPageSize()) + (f.length() % BufferPool.getPageSize() > 0 ? 1 : 0);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to generate this tableid
     * somewhere to ensure that each HeapFile has a "unique id," and that you always return the same value for a
     * particular HeapFile. We suggest hashing the absolute file name of the file underlying the heapfile, i.e.
     * f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.struct;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file))) {
            Preconditions.checkArgument(pid instanceof HeapPageId, "the input page id must be heap page!");
            int start = pid.getPageNumber() * BufferPool.getPageSize();
            byte[] pageBuf = new byte[BufferPool.getPageSize()];
            bis.skip(start);
            int size = bis.read(pageBuf, 0, BufferPool.getPageSize());
            if (size == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (size < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from BTreeFile");
            }
            Debug.log(1, "HeapFile.readPage: read page %d", pid.getPageNumber());
            return new HeapPage((HeapPageId) pid, pageBuf);
        } catch (FileNotFoundException e) {
            Debug.log("file not found:%s", file.getName());
            return null;
        } catch (IOException ioe) {
            Debug.log("read file failed:%s", file.getName());
            return null;
        } catch (IndexOutOfBoundsException ioe) {
            Debug.log("read file out of bound:%s", file.getName());
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        Preconditions.checkArgument(page instanceof HeapPage, "the input page id must be heap page!");
        Files.write(file.toPath(), page.getPageData(), StandardOpenOption.APPEND);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeadFileIterator(tid);
    }

    class HeadFileIterator extends AbstractDbFileIterator {

        /**
         * the transaction id
         */
        private TransactionId tid;
        /**
         * the current page
         */
        private HeapPage current;
        /**
         * the iterator of tuple
         */
        private Iterator<Tuple> it;

        public HeadFileIterator(TransactionId tid) {
            this.tid = tid;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId pageId = new HeapPageId(tableId, 0);
            current = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            it = current.iterator();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            while (it != null) {
                if (it.hasNext()) {
                    Tuple item = it.next();
                    return item;
                } else {
                    HeapPageId pageId = current.nextPage();
                    if (pageId.getPageNumber() >= pageSize) {
                        return null;
                    }
                    current = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                    it = current.iterator();
                }
            }
            return null;
        }

        /**
         * If subclasses override this, they should call super.close().
         */
        @Override
        public void close() {
            super.close();
            it = null;
            current = null;
        }
    }



}

