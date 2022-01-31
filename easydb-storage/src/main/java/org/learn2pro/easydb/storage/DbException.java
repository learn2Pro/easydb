package org.learn2pro.easydb.storage;

import java.security.PrivilegedActionException;

/** Generic database exception class */
public class DbException extends Exception {
    private static final long serialVersionUID = 1L;

    public DbException(String s) {
        super(s);
    }

    /**
     * Constructs a new exception with the specified cause and a detail message of <tt>(cause==null ? null :
     * cause.toString())</tt> (which typically contains the class and detail message of <tt>cause</tt>). This constructor is
     * useful for exceptions that are little more than wrappers for other throwables (for example, {@link
     * PrivilegedActionException}).
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()} method).  (A
     *         <tt>null</tt> value is permitted, and indicates that the cause is nonexistent or unknown.)
     * @since 1.4
     */
    public DbException(Throwable cause) {
        super(cause);
    }
}
