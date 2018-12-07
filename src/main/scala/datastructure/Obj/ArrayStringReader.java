package datastructure.Obj;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class ArrayStringReader extends Reader {
    private Iterator<String> buffList;
    private String current;
    private int length;
    private int next = 0;

    public ArrayStringReader(Iterator<String> list) {
        this.buffList = list;
        if (buffList.hasNext())
            current = buffList.next();
        else current = "";
        length = current.length();
    }

    /**
     * Check to make sure that the stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (current == null || buffList == null)
            throw new IOException("Stream closed");
    }

    /**
     * Reads a single character.
     *
     * @return The character read, or -1 if the end of the stream has been
     * reached
     * @throws IOException If an I/O error occurs
     */
    public int read() throws IOException {
        synchronized (lock) {
            ensureOpen();
            if (next >= length) {
                if (buffList.hasNext()) swichToNext();
                else return -1;
            } else {
                return current.charAt(next++);
            }
            return read();
        }
    }

    /**
     * Reads characters into a portion of an array.
     *
     * @param cbuf Destination buffer
     * @param off  Offset at which to start writing characters
     * @param len  Maximum number of characters to read
     * @return The number of characters read, or -1 if the end of the
     * stream has been reached
     * @throws IOException If an I/O error occurs
     */
    public int read(char cbuf[], int off, int len) throws IOException {
        synchronized (lock) {
            ensureOpen();
            if ((off < 0) || (off > cbuf.length) || (len < 0) ||
                    ((off + len) > cbuf.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            } else if (len == 0) {
                return 0;
            }
            if (next >= length) {
                if (!buffList.hasNext())
                    return -1;
                else {
                    swichToNext();
                }
            }
            if (length - next < len) {
                int read = length - next;
                current.getChars(next, next + read, cbuf, off);
                next += read;
                return length - next + read(cbuf, off + read, len - read);
            } else {
                int n = Math.min(length - next, len);
                current.getChars(next, next + n, cbuf, off);
                next += n;
                return n;
            }
        }
    }

    /**
     * Skips the specified number of characters in the stream. Returns
     * the number of characters that were skipped.
     *
     * <p>The <code>ns</code> parameter may be negative, even though the
     * <code>skip</code> method of the {@link Reader} superclass throws
     * an exception in this case. Negative values of <code>ns</code> cause the
     * stream to skip backwards. Negative return values indicate a skip
     * backwards. It is not possible to skip backwards past the beginning of
     * the string.
     *
     * <p>If the entire string has been read or skipped, then this method has
     * no effect and always returns 0.
     *
     * @throws IOException If an I/O error occurs
     */
    public long skip(long ns) throws IOException {
        if (ns < 0) throw new IOException(); //can't roll back
        synchronized (lock) {
            ensureOpen();
            if (next >= length) {
                if (!this.buffList.hasNext()) return 0;
                else swichToNext();
            }
            // Bound skip by beginning and end of the source
            if ((length - next) < ns) {
                swichToNext();
                return skip(ns - length + next);
            }
            next += ns;
            return ns;
        }
    }

    public void swichToNext() {
        if (buffList.hasNext()) current = "\r\n" + buffList.next();
        else current = "\r" + "";

        this.length = current.length();
        this.next = 0;
    }

    /**
     * Tells whether this stream is ready to be read.
     *
     * @return True if the next read() is guaranteed not to block for input
     * @throws IOException If the stream is closed
     */
    public boolean ready() throws IOException {
        synchronized (lock) {
            ensureOpen();
            return true;
        }
    }

    /**
     * Tells whether this stream supports the mark() operation, which it does.
     */
    public boolean markSupported() {
        return false;
    }

    /**
     * Marks the present position in the stream.  Subsequent calls to reset()
     * will reposition the stream to this point.
     *
     * @param readAheadLimit Limit on the number of characters that may be
     *                       read while still preserving the mark.  Because
     *                       the stream's input comes from a string, there
     *                       is no actual limit, so this argument must not
     *                       be negative, but is otherwise ignored.
     * @throws IllegalArgumentException If {@code readAheadLimit < 0}
     * @throws IOException              If an I/O error occurs
     */
    public void mark(int readAheadLimit) throws IOException {
        throw new IOException("don't support mark");
    }

    /**
     * Resets the stream to the most recent mark, or to the beginning of the
     * string if it has never been marked.
     *
     * @throws IOException If an I/O error occurs
     */
    public void reset() throws IOException {
        throw new IOException("don't support reset");
    }

    /**
     * Closes the stream and releases any system resources associated with
     * it. Once the stream has been closed, further read(),
     * ready(), mark(), or reset() invocations will throw an IOException.
     * Closing a previously closed stream has no effect.
     */
    public void close() {
        buffList = null;
        current = null;
    }

}
